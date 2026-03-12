package main

import (
	"context"
	"fmt"
	"lgworker/gen/combat_events"
	"lgworker/gen/loggingway_rpc"
	"math"
	"slices"
	"sort"

	"github.com/golang/protobuf/proto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PlayerStats struct {
	Name                 string
	ID                   uint64
	ContentID            uint64
	jobId                uint32
	Level                uint32
	attackPower          uint32
	attackMagicPotency   uint32
	weaponPhysicalDamage uint32
	weaponMagicalDamage  uint32
	weaponHQ             uint32

	//attributes
	tenacity      uint32
	skillspeed    uint32
	spellspeed    uint32
	determination uint32
	criticalHit   uint32
	directHit     uint32

	//Calculated fields
	isCaster         bool
	isTank           bool
	isPhysicalRanged bool
	enochianActive   bool
	darksideActive   bool
	//Derived statistics
	TotalDamage     uint64
	TotalHealing    uint64
	TotalCrits      uint64
	TotalDirectHits uint64
	TotalHits       uint64

	ActionBreakdown map[uint32]*ActionStats

	FirstTimestamp int64
	LastTimestamp  int64
}
type ActionStats struct {
	ActionID   uint32
	Hits       uint64
	Crits      uint64
	DirectHits uint64
	Damage     uint64
}

type CombatStats struct {
	Players map[uint64]*PlayerStats
	Start   int64
	End     int64
}

func ParseCombatEvents(
	ctx context.Context,
	conn *pgxpool.Conn,
	payload []byte,
) error {

	var batch loggingway_rpc.NewEncounterRequest
	err := proto.Unmarshal(payload, &batch)
	if err != nil {
		println("failed to unmarshal payload:%v", err)
		//should find some redis way to mark a message as unprocessable/persist for debugging
		return err
	}
	events := batch.Events
	reportID := batch.ReportId
	stats := &CombatStats{
		Players: make(map[uint64]*PlayerStats),
	}
	start := events[0].GetEncounterStart()
	zone_id := start.Territorytype
	for _, e := range events {

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if stats.Start == 0 || e.TimestampEpochMs < stats.Start {
			stats.Start = e.TimestampEpochMs
		}
		if e.TimestampEpochMs > stats.End {
			stats.End = e.TimestampEpochMs
		}

		switch data := e.EventData.(type) {

		case *combat_events.CombatEvent_DamageTaken:
			handleDamage(stats, e, data.DamageTaken)

		case *combat_events.CombatEvent_Healed:
			handleHealing(stats, e, data.Healed)

		case *combat_events.CombatEvent_PlayerJoin:
			handleJoin(stats, e, data.PlayerJoin)
		}

	}

	computeDerivedStats(stats)

	printStats(stats)

	err = commitToDatabase(reportID, int16(zone_id), payload, stats, conn, ctx)
	if err != nil {
		return err
	}
	return nil

}

func handleJoin(stats *CombatStats, e *combat_events.CombatEvent, join *combat_events.PlayerEnterCombat) {
	player := getOrCreatePlayer(stats, e.Source, e.TimestampEpochMs)
	//all the fields that can't be assigned by entity are here
	player.Name = join.Name
	//Attributes
	player.attackPower = join.AttackPower
	player.attackMagicPotency = join.AttackPower //TODO:add magic
	player.criticalHit = join.CriticalHit
	player.directHit = join.DirectHit
	player.determination = join.Determination
	player.skillspeed = join.Skillspeed
	player.spellspeed = join.Spellspeed
	player.tenacity = join.Tenacity
	//others
	player.jobId = join.JobId
	player.ContentID = join.ContentId
	//job helper flags resolution,lazy
	player.isCaster = slices.Contains(Casters, join.JobId)
	player.isPhysicalRanged = slices.Contains(PhysicalRanged, join.JobId)
	player.isTank = slices.Contains(Tanks, join.JobId)
}

func handleDamage(
	stats *CombatStats,
	e *combat_events.CombatEvent,
	dmg *combat_events.DamageTakenData,
) {

	if e.Source == nil || e.Source.Objectkind != combat_events.ObjectKind_Player {
		return
	}

	player := getOrCreatePlayer(stats, e.Source, e.TimestampEpochMs)

	var levelModifier = levelModifiers[player.Level]
	var attackPower = player.attackPower //attackPower is auto selected at parse time for the correct type(phys or magic)
	var weaponDamage = math.Floor(float64(player.weaponPhysicalDamage+player.weaponHQ+levelModifier.main*jobDamageModifiers[player.jobId]/1000.0)) / 100.0
	if player.isCaster {
		weaponDamage = math.Floor(float64(player.weaponMagicalDamage+player.weaponHQ+levelModifier.main*jobDamageModifiers[player.jobId]/1000.0)) / 100.0
	}
	var levelAttackModifier = float32((player.Level-90))*4.2 + 195
	if player.isTank {
		levelAttackModifier = float32((player.Level-90))*3.4 + 156
	}
	var attack = math.Floor(float64(100.0+levelAttackModifier*(float32(attackPower)-float32(levelModifier.main))/float32(levelModifier.main)) / 100)
	var magical = 0
	if dmg.DamageType == combat_events.DamageType_DAMAGE_TYPE_MAGIC {
		magical = 1
	}
	var speedMultiplier = 1.0
	var skillSpeedModifier = 1000.0 + math.Ceil(float64(130.0*(levelModifier.sub-player.skillspeed)/levelModifier.div))
	var spellSpeedModifier = 1000.0 + math.Ceil(float64(130.0*(levelModifier.sub-player.spellspeed)/levelModifier.div))
	if slices.Contains(GCD, dmg.ActionId) && !slices.Contains(affectedRecasts, dmg.ActionId) { // Only GCDs whose recasts are above 1.5s seem to be affected by SPS and SKS. Some specific GCDs aren't affected, but they (as far as I could tell) *always* say that in their description at the very bottom.
		recast := GCDrecast[dmg.ActionId] //GCD are always 25, unless you wanted the recast counting player stats?
		if magical == 1 {
			speedMultiplier = math.Floor(math.Floor(spellSpeedModifier*recast)/10.0) / 100.0 / recast
		} else {
			speedMultiplier = math.Floor((math.Floor(skillSpeedModifier*recast) / 10.0)) / 100.0 / recast
		}
	}
	var buffMultiplier = math.Floor(100*attack*weaponDamage) / 100
	if player.isCaster {
		buffMultiplier *= 1.3
	} else if player.isPhysicalRanged {
		buffMultiplier *= 1.2
	} else if player.isTank {
		buffMultiplier *= math.Floor(float64(112.0*(player.tenacity-levelModifier.sub)/levelModifier.div) / 1000.0)
	}
	buffMultiplier *= 1.0 + math.Floor(float64(140.0*(player.determination-levelModifier.main)/levelModifier.div)/1000.0)
	var criticalMultiplier = math.Floor(float64(200.0*(player.criticalHit-levelModifier.sub)/levelModifier.div+1400) / 1000.0)

	if player.enochianActive { // These cases are for buffs that don't count as status effects for some reason. They can be read from the gauge.
		buffMultiplier *= 1.27
	} else if player.darksideActive {
		buffMultiplier *= 1.1
	}
	var guaranteedCriticalHit = slices.Contains(guaranteedCriticalHits, dmg.ActionId)
	var guaranteedDirectHit = slices.Contains(guaranteedDirectHits, dmg.ActionId)
	var innerRelease = false
	var internalBuffMultiplier = 1.0
	var internalCriticalHitRateMultiplier = 1.0
	var internalDirectHitRateMultiplier = 1.0

	/*

		Below, I introduce a few tables to reference for buffs: buffs (Divination, Brotherhood, etc.), targetBuffs (Chain Stratagem and Dokumori), criticalHitBuffs (Devilment, Battle Litany, etc.), and directHitBuffs (Devilment, Battle Voice, etc.).

		The first two map the status Ids to a pair of numbers (physical and magical damage). The buffs should be in decimal format. For example, Divination is a 6% buff, so it should be (1.06, 1.06), as it buffs both physical and magical damage. Embolden (on the RDM who used it), however, only buffs magical damage by 10%, so it'd be (1.0, 1.1); on everyone else, Embolden is represented as (1.05, 1.05).

		The other two tables just map the status Ids to the CH/DH rate increase.

		I also introduce a falloff table (named falloffs) that has the action's falloff. For this, we also need to add whether it's the main target to DamageTakenData.

		Finally, I assume that the possible potencies for an ability are stored in "potencies." For example, if you can break combo with an action, both of those potencies would be included. We'd also include positionals as other possible potencies.
	*/

	if e.SourceSnapshot != nil {
		for _, effect := range e.SourceSnapshot.StatusEffects {
			if effect.Id == specificStatusEffect["Inner Release"] {
				innerRelease = true
				break
			}
		}
		for _, effect := range e.SourceSnapshot.StatusEffects {
			buff, exists := buffs[effect.Id] // This is for the other buffs.
			if exists {
				if effect.SourceId == uint32(player.ID) {
					internalBuffMultiplier *= float64(buff[1])
				}
				buffMultiplier *= float64(buff[1])
			}
			if dmg.Crit {
				buff, exists := criticalHitBuffs[effect.Id]
				if exists {
					if effect.SourceId == uint32(player.ID) {
						internalCriticalHitRateMultiplier *= buff
					}
					if guaranteedCriticalHit || ((dmg.ActionId == specificActions["Fell Cleave"] || dmg.ActionId == specificActions["Decimate"]) && innerRelease) { // We need to find the Ids or store/reference the ability names...
						if effect.SourceId == uint32(player.ID) {
							internalBuffMultiplier *= buff
						}
						buffMultiplier *= buff
					}
				}
			}
			if dmg.DirectHit {
				buff, exists := directHitBuffs[effect.Id]
				if exists {
					if effect.SourceId == uint32(player.ID) {
						internalDirectHitRateMultiplier *= buff
					}
					if guaranteedCriticalHit || ((dmg.ActionId == specificActions["Fell Cleave"] || dmg.ActionId == specificActions["Decimate"]) && innerRelease) {
						if effect.SourceId == uint32(player.ID) {
							internalBuffMultiplier *= buff
						}
						buffMultiplier *= buff
					}
				}
			}
		}
	}
	if e.TargetSnapshot != nil {
		for _, effect := range e.TargetSnapshot.StatusEffects {
			buff, exists := targetBuffs[effect.Id] // This is for Chain Stratagem and Dokumori.
			if exists {
				if effect.SourceId == uint32(player.ID) {
					internalBuffMultiplier *= buff
				}
				buffMultiplier *= buff
			}
			if dmg.Crit {
				buff, exists := criticalHitBuffs[effect.Id]
				if exists {
					if effect.SourceId == uint32(player.ID) {
						internalCriticalHitRateMultiplier *= buff
					}
					if guaranteedCriticalHit || ((dmg.ActionId == specificActions["Fell Cleave"] || dmg.ActionId == specificActions["Decimate"]) && innerRelease) { // We need to find the Ids or store/reference the ability names...
						if effect.SourceId == uint32(player.ID) {
							internalBuffMultiplier *= buff
						}
						buffMultiplier *= buff
					}
				}
			}
			if dmg.DirectHit {
				buff, exists := directHitBuffs[effect.Id]
				if exists {
					if effect.SourceId == uint32(player.ID) {
						internalDirectHitRateMultiplier *= buff
					}
					if guaranteedCriticalHit || ((dmg.ActionId == specificActions["Fell Cleave"] || dmg.ActionId == specificActions["Decimate"]) && innerRelease) {
						if effect.SourceId == uint32(player.ID) {
							internalBuffMultiplier *= buff
						}
						buffMultiplier *= buff
					}
				}
			}
		}
	}

	var estimatedPotency = float64(dmg.Amount) / buffMultiplier

	/*if !dmg.MainTarget {
		falloff, exists := falloffs[dmg.ActionId] // For example, RDM's Resolution does 55% less damage to all other targets. This means that falloffs[resolutionId] == 0.55.
		if exists {
			estimatedPotency /= 1.0 - falloff
		}
	}*/

	if dmg.Crit {
		estimatedPotency /= criticalMultiplier
	}
	if dmg.DirectHit {
		estimatedPotency /= 1.25
	}

	// We should also divide estimatedPotency by the damage down multiplier if the player has a damage down. We do not know the damage down multiplier directly, but we can estimate it. For example, if it's 15%, we'd divide estimatedPotency by 0.85.
	// Weakness affects the character's statistics directly, so it's already taken into account with the earlier math.

	validPotencies, exists := potencies[dmg.ActionId]
	if exists {
		var best = 1000000.0
		var distance = 1000000.0
		for potency := range validPotencies[dmg.ActionId] {
			var newDistance = math.Abs(float64(potency) - estimatedPotency)
			if newDistance < distance {
				best = float64(potency)
				distance = newDistance
			}
		}
		estimatedPotency = best
	}

	if guaranteedCriticalHit || ((dmg.ActionId == specificActions["Fell Cleave"] || dmg.ActionId == specificActions["Decimate"]) && innerRelease) {
		estimatedPotency *= 1.6 // To avoid gear bias, we're using a fixed increase for CHs. This is the usual damage increase from critical hit rate.
	} else {
		estimatedPotency += (estimatedPotency*1.6*0.25*internalCriticalHitRateMultiplier + estimatedPotency*(1.0-0.25*internalCriticalHitRateMultiplier)) - (estimatedPotency*1.6*0.25 + estimatedPotency*(1.0-0.25)) // This is just math to get the expected damage increase from critical rate increases (i.e., treating it as a damage buff). If the multiplier is 1.0, it doesn't actually do anything.
	}
	if guaranteedDirectHit || ((dmg.ActionId == specificActions["Fell Cleave"] || dmg.ActionId == specificActions["Decimate"]) && innerRelease) {
		estimatedPotency *= 1.25
	} else {
		estimatedPotency += (estimatedPotency*1.25*0.33*internalDirectHitRateMultiplier + estimatedPotency*(1.0-0.33*internalDirectHitRateMultiplier)) - (estimatedPotency*1.25*0.33 + estimatedPotency*(1.0-0.33))
	}

	estimatedPotency *= speedMultiplier

	player.TotalDamage += uint64(100.0 * estimatedPotency) // This is just to make the potency number bigger.
	player.TotalHits++

	if dmg.Crit {
		player.TotalCrits++
	}
	if dmg.DirectHit {
		player.TotalDirectHits++
	}

	// Action breakdown
	action := player.ActionBreakdown[dmg.ActionId]
	if action == nil {
		action = &ActionStats{
			ActionID: dmg.ActionId,
		}
		player.ActionBreakdown[dmg.ActionId] = action
	}

	action.Hits++
	action.Damage += uint64(100.0 * estimatedPotency)
	if dmg.Crit {
		action.Crits++
	}
	if dmg.DirectHit {
		action.DirectHits++
	}
}

func handleHealing(
	stats *CombatStats,
	e *combat_events.CombatEvent,
	heal *combat_events.HealedData,
) {

	if e.Source == nil || e.Source.Objectkind != combat_events.ObjectKind_Player {
		return
	}

	player := getOrCreatePlayer(stats, e.Source, e.TimestampEpochMs)

	player.TotalHealing += uint64(heal.Amount)
}

func getOrCreatePlayer(
	stats *CombatStats,
	entity *combat_events.Entity,
	ts int64,
) *PlayerStats {

	p, exists := stats.Players[entity.GameobjectId]
	if exists {
		p.LastTimestamp = ts
		return p
	}

	p = &PlayerStats{
		ID:              entity.GameobjectId,
		ActionBreakdown: make(map[uint32]*ActionStats),
		FirstTimestamp:  ts,
		LastTimestamp:   ts,
	}

	stats.Players[entity.GameobjectId] = p
	return p
}

func computeDerivedStats(stats *CombatStats) {

	fightDuration := float64(stats.End-stats.Start) / 1000.0
	if fightDuration <= 0 {
		fightDuration = 1
	}

	for _, p := range stats.Players {

		playerDuration := float64(p.LastTimestamp-p.FirstTimestamp) / 1000.0
		if playerDuration <= 0 {
			playerDuration = fightDuration
		}

		dps := float64(p.TotalDamage) / playerDuration
		critRate := 0.0
		if p.TotalHits > 0 {
			critRate = float64(p.TotalCrits) / float64(p.TotalHits) * 100
		}

		fmt.Printf("Player: %s\n", p.Name)
		fmt.Printf("  Total Damage: %d\n", p.TotalDamage)
		fmt.Printf("  DPS: %.2f\n", dps)
		fmt.Printf("  Crit Rate: %.2f%%\n", critRate)
		fmt.Printf("  Total Healing: %d\n", p.TotalHealing)
		fmt.Println()
	}
}
func commitToDatabase(reportid int64, zone_id int16, payload []byte, stats *CombatStats, conn *pgxpool.Conn, ctx context.Context) error {
	//commit encounter
	fmt.Println("Start transaction...")
	tx, err := conn.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:       pgx.Serializable,
		AccessMode:     pgx.ReadWrite,
		DeferrableMode: pgx.NotDeferrable,
	})
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx) //auto-fails if tx is commited before this fct returns
	encounter_query := `INSERT INTO encounters (report_id,zone_id,payload)
		VALUES ($1, $2, $3)
		RETURNING id;
	`
	var encounterID int64
	fmt.Println("Creating encounter...")
	tx.QueryRow(ctx, encounter_query, reportid, zone_id, payload).Scan(&encounterID)
	fmt.Printf("ID:%v", encounterID)
	for _, p := range stats.Players {

		duration := float64(p.LastTimestamp-p.FirstTimestamp) / 1000.0
		if duration <= 0 {
			duration = 1
		}

		dps := float64(p.TotalDamage) / duration
		hps := float64(p.TotalHealing) / duration

		critRate := 0.0
		directRate := 0.0

		if p.TotalHits > 0 {
			critRate = float64(p.TotalCrits) / float64(p.TotalHits)
			directRate = float64(p.TotalDirectHits) / float64(p.TotalHits)
		}
		fmt.Printf("Inserting encounter stats for:%v \n", p.Name)
		_, err := tx.Exec(ctx, `
        INSERT INTO encounter_player_stats (
            encounter_id,
            player_id,
            player_name,
            total_damage,
            total_healing,
            total_hits,
            total_crits,
            total_direct_hits,
            first_timestamp,
            last_timestamp,
            duration_seconds,
            dps,
            hps,
            crit_rate,
            direct_hit_rate
        )
        VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15)
        ON CONFLICT (encounter_id, player_id) DO NOTHING
    `,
			encounterID,
			p.ID,
			p.Name,
			p.TotalDamage,
			p.TotalHealing,
			p.TotalHits,
			p.TotalCrits,
			p.TotalDirectHits,
			p.FirstTimestamp,
			p.LastTimestamp,
			duration,
			dps,
			hps,
			critRate,
			directRate,
		)
		if err != nil {
			return err
		}

		// Insert action breakdown
		for _, a := range p.ActionBreakdown {

			actionCrit := 0.0
			actionDirect := 0.0

			if a.Hits > 0 {
				actionCrit = float64(a.Crits) / float64(a.Hits)
				actionDirect = float64(a.DirectHits) / float64(a.Hits)
			}
			fmt.Printf("Inserting action breakdown for:%v, actionID:%v \n", p.Name, a.ActionID)
			_, err := tx.Exec(ctx, `
            INSERT INTO encounter_player_action_stats (
                encounter_id,
                player_id,
                action_id,
                total_damage,
                hits,
                crits,
                direct_hits,
                crit_rate,
                direct_hit_rate
            )
            VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9)
            ON CONFLICT (encounter_id, player_id, action_id) DO NOTHING
        `,
				encounterID,
				p.ID,
				a.ActionID,
				a.Damage,
				a.Hits,
				a.Crits,
				a.DirectHits,
				actionCrit,
				actionDirect,
			)
			if err != nil {
				return err
			}
		}
	}
	tx.Commit(ctx)
	return nil
}

func printStats(stats *CombatStats) {

	fightDuration := float64(stats.End-stats.Start) / 1000.0
	if fightDuration <= 0 {
		fightDuration = 1
	}

	fmt.Println("===================================")
	fmt.Printf("Fight Duration: %.2f seconds\n", fightDuration)
	fmt.Println("===================================")

	// Convert map to slice for sorting by total damage
	players := make([]*PlayerStats, 0, len(stats.Players))
	for _, p := range stats.Players {
		players = append(players, p)
	}

	sort.Slice(players, func(i, j int) bool {
		return players[i].TotalDamage > players[j].TotalDamage
	})

	for _, p := range players {

		playerDuration := float64(p.LastTimestamp-p.FirstTimestamp) / 1000.0
		if playerDuration <= 0 {
			playerDuration = fightDuration
		}

		dps := float64(p.TotalDamage) / playerDuration
		hps := float64(p.TotalHealing) / playerDuration

		critRate := 0.0
		directRate := 0.0

		if p.TotalHits > 0 {
			critRate = float64(p.TotalCrits) / float64(p.TotalHits) * 100
			directRate = float64(p.TotalDirectHits) / float64(p.TotalHits) * 100
		}

		fmt.Println("-----------------------------------")
		fmt.Printf("Player: %s (ID: %d)\n", p.Name, p.ID)
		fmt.Printf("  Total Damage: %d\n", p.TotalDamage)
		fmt.Printf("  DPS: %.2f\n", dps)
		fmt.Printf("  Total Healing: %d\n", p.TotalHealing)
		fmt.Printf("  HPS: %.2f\n", hps)
		fmt.Printf("  Hits: %d\n", p.TotalHits)
		fmt.Printf("  Crit Rate: %.2f%%\n", critRate)
		fmt.Printf("  Direct Hit Rate: %.2f%%\n", directRate)

		// ---- Action Breakdown ----
		if len(p.ActionBreakdown) > 0 {
			fmt.Println("  Action Breakdown:")

			actions := make([]*ActionStats, 0, len(p.ActionBreakdown))
			for _, a := range p.ActionBreakdown {
				actions = append(actions, a)
			}

			sort.Slice(actions, func(i, j int) bool {
				return actions[i].Damage > actions[j].Damage
			})

			for _, a := range actions {

				actionCritRate := 0.0
				actionDirectRate := 0.0

				if a.Hits > 0 {
					actionCritRate = float64(a.Crits) / float64(a.Hits) * 100
					actionDirectRate = float64(a.DirectHits) / float64(a.Hits) * 100
				}

				fmt.Printf(
					"    Action %d | Damage: %d | Hits: %d | Crit%%: %.2f | Direct%%: %.2f\n",
					a.ActionID,
					a.Damage,
					a.Hits,
					actionCritRate,
					actionDirectRate,
				)
			}
		}
	}

	fmt.Println("===================================")
}
