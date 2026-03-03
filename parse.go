package main

import (
	"context"
	"fmt"
	"lgworker/gen/combat_events"
	"lgworker/gen/loggingway_rpc"
	"sort"

	"github.com/golang/protobuf/proto"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PlayerStats struct {
	Name string
	ID   uint64

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

func handleDamage(
	stats *CombatStats,
	e *combat_events.CombatEvent,
	dmg *combat_events.DamageTakenData,
) {

	if e.Source == nil || e.Source.Objectkind != combat_events.ObjectKind_Player {
		return
	}

	player := getOrCreatePlayer(stats, e.Source, e.TimestampEpochMs)

	player.TotalDamage += uint64(dmg.Amount)
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
	action.Damage += uint64(dmg.Amount)
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
		Name:            entity.Name,
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
