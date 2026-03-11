package main

import (
	"fmt"
	"log"
	"os"

	"github.com/gocarina/gocsv"
)

// Load sheet.csv stuff in memory
type ActionData struct {
	Index                   uint32 `csv:"#"`
	Name                    string `csv:"Name"`
	UnlockLink              string `csv:"UnlockLink"`
	Icon                    string `csv:"Icon"`
	VFX                     string `csv:"VFX"`
	ActionTimelineHit       string `csv:"ActionTimelineHit"`
	PrimaryCostValue        string `csv:"PrimaryCostValue"`
	SecondaryCostValue      string `csv:"SecondaryCostValue"`
	ActionCombo             string `csv:"ActionCombo"`
	Cast100ms               string `csv:"Cast100ms"`
	Recast100ms             string `csv:"Recast100ms"`
	ActionProcStatus        string `csv:"ActionProcStatus"`
	StatusGainSelf          string `csv:"StatusGainSelf"`
	Omen                    string `csv:"Omen"`
	OmenAlt                 string `csv:"OmenAlt"`
	AnimationEnd            string `csv:"AnimationEnd"`
	ActionCategory          string `csv:"ActionCategory"`
	Unknown1                string `csv:"Unknown1"`
	AnimationStart          string `csv:"AnimationStart"`
	Unknown2                string `csv:"Unknown2"`
	BehaviourType           string `csv:"BehaviourType"`
	ClassJobLevel           string `csv:"ClassJobLevel"`
	CastType                string `csv:"CastType"`
	EffectRange             string `csv:"EffectRange"`
	XAxisModifier           string `csv:"XAxisModifier"`
	PrimaryCostType         string `csv:"PrimaryCostType"`
	SecondaryCostType       string `csv:"SecondaryCostType"`
	ExtraCastTime100ms      string `csv:"ExtraCastTime100ms"`
	CooldownGroup           string `csv:"CooldownGroup"`
	AdditionalCooldownGroup string `csv:"AdditionalCooldownGroup"`
	MaxCharges              string `csv:"MaxCharges"`
	Aspect                  string `csv:"Aspect"`
	Unknown4                string `csv:"Unknown4"`
	ClassJobCategory        string `csv:"ClassJobCategory"`
	AutoAttackBehaviour     string `csv:"AutoAttackBehaviour"`
	EquivalenceGroup        string `csv:"EquivalenceGroup"`
	Unknown70               string `csv:"Unknown_70"`
	ClassJob                string `csv:"ClassJob"`
	Range                   string `csv:"Range"`
	DeadTargetBehaviour     string `csv:"DeadTargetBehaviour"`
	AttackType              string `csv:"AttackType"`
	Unknown8                string `csv:"Unknown8"`
	IsRoleAction            bool   `csv:"IsRoleAction"`
	Unknown28               string `csv:"Unknown28"`
	CanTargetSelf           bool   `csv:"CanTargetSelf"`
	CanTargetParty          bool   `csv:"CanTargetParty"`
	CanTargetAlliance       bool   `csv:"CanTargetAlliance"`
	CanTargetHostile        bool   `csv:"CanTargetHostile"`
	CanTargetAlly           bool   `csv:"CanTargetAlly"`
	Unknown10               string `csv:"Unknown10"`
	TargetArea              bool   `csv:"TargetArea"`
	CanTargetOwnPet         bool   `csv:"CanTargetOwnPet"`
	CanTargetPartyPet       bool   `csv:"CanTargetPartyPet"`
	RequiresLineOfSight     bool   `csv:"RequiresLineOfSight"`
	NeedToFaceTarget        bool   `csv:"NeedToFaceTarget"`
	Unknown14               string `csv:"Unknown14"`
	PreservesCombo          bool   `csv:"PreservesCombo"`
	Unknown15               string `csv:"Unknown15"`
	AffectsPosition         bool   `csv:"AffectsPosition"`
	IsPvP                   bool   `csv:"IsPvP"`
	Unknown16               string `csv:"Unknown16"`
	LogCastMessage          string `csv:"LogCastMessage"`
	Unknown18               string `csv:"Unknown18"`
	LogMissMessage          string `csv:"LogMissMessage"`
	LogActionMessage        string `csv:"LogActionMessage"`
	Unknown21               string `csv:"Unknown21"`
	Unknown22               string `csv:"Unknown22"`
	Unknown23               string `csv:"Unknown23"`
	CanUseWhileMounted      bool   `csv:"CanUseWhileMounted"`
	Unknown25               string `csv:"Unknown25"`
	IsPlayerAction          bool   `csv:"IsPlayerAction"`
	Unknown27               string `csv:"Unknown27"`
}

type StatusData struct {
	Index              string `csv:"#"`
	Name               string `csv:"Name"`
	Description        string `csv:"Description"`
	Icon               string `csv:"Icon"`
	ParamModifier      string `csv:"ParamModifier"`
	VFX                string `csv:"VFX"`
	Log                string `csv:"Log"`
	Unknown0           string `csv:"Unknown0"`
	MaxStacks          string `csv:"MaxStacks"`
	ClassJobCategory   string `csv:"ClassJobCategory"`
	StatusCategory     string `csv:"StatusCategory"`
	HitEffect          string `csv:"HitEffect"`
	PartyListPriority  string `csv:"PartyListPriority"`
	CanIncreaseRewards bool   `csv:"CanIncreaseRewards"`
	ParamEffect        string `csv:"ParamEffect"`
	TargetType         string `csv:"TargetType"`
	Flags              string `csv:"Flags"`
	Flag2              string `csv:"Flag2"`
	Unknown701         string `csv:"Unknown_70_1"`
	Unknown2           string `csv:"Unknown2"`
	LockMovement       bool   `csv:"LockMovement"`
	Unknown3           string `csv:"Unknown3"`
	LockActions        bool   `csv:"LockActions"`
	LockControl        bool   `csv:"LockControl"`
	Transfiguration    bool   `csv:"Transfiguration"`
	IsGaze             bool   `csv:"IsGaze"`
	CanDispel          bool   `csv:"CanDispel"`
	InflictedByActor   bool   `csv:"InflictedByActor"`
	IsPermanent        bool   `csv:"IsPermanent"`
	NoLogVfx           bool   `csv:"NoLogVfx"`
	Unknown5           string `csv:"Unknown5"`
	CanStatusOff       bool   `csv:"CanStatusOff"`
	IsFcBuff           bool   `csv:"IsFcBuff"`
	Invisibility       bool   `csv:"Invisibility"`
	Unknown6           string `csv:"Unknown6"`
	Unknown702         string `csv:"Unknown_70_2"`
	Unknown7           string `csv:"Unknown7"`
}

func LoadCSVs() {
	fmt.Println("Loading Action.csv...")
	actionFile, err := os.OpenFile("Action.csv", os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("could not load action file: %v", err)
	}
	defer actionFile.Close()
	actions := []*ActionData{}
	if err := gocsv.UnmarshalFile(actionFile, &actions); err != nil {
		log.Fatalf("failed to unmarshal:%v", err)
	}
	for _, action := range actions {
		if action.CooldownGroup == "58" { //58 is the CD group for the GCD
			GCD = append(GCD, action.Index)
		}
	}
	fmt.Println("Loading Status.csv...")
	statusFile, err := os.OpenFile("Status.csv", os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Fatalf("could not load status file: %v", err)
	}
	defer statusFile.Close()
	statuses := []*StatusData{}
	if err := gocsv.UnmarshalFile(actionFile, &statuses); err != nil {
		log.Fatalf("failed to unmarshal:%v", err)
	}
	for _, status := range statuses {

	}
}
