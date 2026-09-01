"""Deterministic, independently selectable MuleGraph investigation cases."""
from __future__ import annotations
import random
from datetime import date, timedelta
import pandas as pd
from src.pipeline import policy

MULE_COLLECTOR="ACC_M_COLLECTOR"; MULE_LOOKALIKE="ACC_M_LOOKALIKE"
MULE_SOURCES=[f"ACC_M_SRC_{i}" for i in range(1,6)]; MULE_DESTINATIONS=[f"ACC_M_DEST_{i}" for i in range(1,4)]
CONTROL_HUB="ACC_C_HUB"; CONTROL_SOURCES=[f"ACC_C_SRC_{i}" for i in range(1,6)]; CONTROL_DESTINATIONS=[f"ACC_C_DEST_{i}" for i in range(1,4)]
DEVICE_MULE_SHARED="DEV_M_001"; DEVICE_MULE_LOOKALIKE="DEV_M_002"; DEVICE_CONTROL_SHARED="DEV_C_001"; TAKEOVER_SESSION_ID="SESS_M_001"
BASELINE_ACCOUNT_COUNT=20; COHORT_MULE="mule_network"; COHORT_CONTROL="control_remittance"; COHORT_BASELINE="baseline"
SCENARIOS=(
 ("simple_transfer","Simple suspicious transfer",MULE_COLLECTOR),
 ("rapid_pass_through","Rapid pass-through money mule","ACC_RAPID_COLLECTOR"),
 ("victim_funnel","Multiple victims funneling into one account","ACC_FUNNEL_COLLECTOR"),
 ("multi_hop","Multi-hop fund movement","ACC_CHAIN_COLLECTOR"),
 ("shared_device_cluster","Shared device/account relationships","ACC_DEVICE_COLLECTOR"),
 ("normal_control","Normal account (not flagged)",CONTROL_HUB),
 ("threshold_boundary","Exact detection-threshold boundary","ACC_BOUNDARY_COLLECTOR"),
 ("insufficient_evidence","Insufficient strict evidence","ACC_THIN_COLLECTOR"),
 ("large_network","Larger connected mule network","ACC_LARGE_COLLECTOR"),)
SCENARIO_SEED_ACCOUNTS=[x[2] for x in SCENARIOS]

def _month_dates(rng, year, month, count):
    """Monthly dates with seeded day jitter; no case is pinned to the 10th."""
    out=[]
    for offset in range(count):
        absolute=year*12+month-1+offset
        out.append(date(absolute//12,absolute%12+1,rng.randint(1,28)))
    return out

def generate_dataset(seed=policy.DEFAULT_SEED):
    rng=random.Random(seed); accounts=[]; devices=[]; links=[]; transfers=[]; sessions=[]; counter=0
    def account(a,role,stype,label,opened=date(2026,1,1),cohort=COHORT_MULE):
        accounts.append(dict(account_id=a,cohort=cohort,account_role=role,open_date=opened,display_name=a.replace("ACC_","").replace("_"," ").title(),scenario_type=stype,scenario_label=label))
    def transfer(a,b,amount,when,channel="ach"):
        nonlocal counter
        counter+=1; transfers.append(dict(txn_id=f"TXN_{counter:05d}",source_account=a,dest_account=b,amount=round(amount,2),txn_date=when,channel=channel))
    def fan(stype,label,collector,ns=4,nd=3,nm=3,inbound=1800,outbound=2400,prefix=None,device="flow",opened=date(2026,1,1),cohort=COHORT_MULE):
        prefix=prefix or collector.removesuffix("_COLLECTOR"); src=[f"{prefix}_SRC_{i}" for i in range(1,ns+1)]; dst=[f"{prefix}_DEST_{i}" for i in range(1,nd+1)]
        account(collector,"collector",stype,label,opened,cohort)
        for a in src: account(a,"fan_in_source",stype,label,date(2025,1,1),cohort)
        for a in dst: account(a,"fan_out_destination",stype,label,date(2025,1,1),cohort)
        dates=_month_dates(rng,2026,3,nm)
        for a in src:
            for d in dates: transfer(a,collector,inbound+rng.uniform(-50,50),d)
        for a in dst:
            for d in dates: transfer(collector,a,outbound+rng.uniform(-50,50),d,"wire")
        if device:
            dev=f"DEV_{stype.upper()}"; devices.append(dict(device_id=dev,first_seen_date=date(2026,2,1)))
            related=src if device=="flow" else [f"{prefix}_DEVICE_ONLY_{i}" for i in range(1,6)]
            for a in related:
                if device!="flow": account(a,"device_only_lookalike",stype,label)
                links.append(dict(link_id=f"LINK_{dev}_{a}",device_id=dev,account_id=a,hub_account_id=collector,linked_date=date(2026,2,2)))
        return src,dst

    src,_=fan(*SCENARIOS[0][:2],MULE_COLLECTOR,ns=5,prefix="ACC_M")
    sessions.append(dict(session_id=TAKEOVER_SESSION_ID,device_id="DEV_SIMPLE_TRANSFER",account_id=src[0],compromise_type="credential_stuffing",session_date=date(2026,2,15),note="Compromised session preceded fund movement."))
    account(MULE_LOOKALIKE,"device_only_lookalike",*SCENARIOS[0][:2]); devices.append(dict(device_id=DEVICE_MULE_LOOKALIKE,first_seen_date=date(2026,2,1))); links.append(dict(link_id="LINK_M_LOOKALIKE",device_id=DEVICE_MULE_LOOKALIKE,account_id=MULE_LOOKALIKE,hub_account_id=MULE_COLLECTOR,linked_date=date(2026,2,2)))
    st,label,col=SCENARIOS[1]; account(col,"collector",st,label)
    for i in range(1,5):
        a=f"ACC_RAPID_SRC_{i}"; account(a,"fan_in_source",st,label)
        for month in (3,4,5): transfer(a,col,1000,date(2026,month,i))
    for i in range(1,4):
        a=f"ACC_RAPID_DEST_{i}"; account(a,"fan_out_destination",st,label)
        for month in (3,4,5): transfer(col,a,2400,date(2026,month,6),"wire")
    fan(*SCENARIOS[2][:2],SCENARIOS[2][2],ns=12,inbound=900)
    _,dst=fan(*SCENARIOS[3][:2],SCENARIOS[3][2],device=None)
    previous=dst[0]
    for i in range(1,4):
        node=f"ACC_CHAIN_HOP_{i}"; account(node,"intermediate",*SCENARIOS[3][:2]); transfer(previous,node,6000,date(2026,5,20+i),"wire"); previous=node
    node="ACC_CHAIN_FINAL"; account(node,"final_destination",*SCENARIOS[3][:2]); transfer(previous,node,5800,date(2026,5,25),"wire")
    fan(*SCENARIOS[4][:2],SCENARIOS[4][2],device="only")
    fan(*SCENARIOS[5][:2],CONTROL_HUB,ns=5,nm=8,inbound=1800,outbound=2500,prefix="ACC_C",opened=date(2022,1,1),cohort=COHORT_CONTROL)
    st,label,col=SCENARIOS[6]; account(col,"collector",st,label)
    for i in range(1,policy.FAN_IN_MIN_SOURCES+1):
        a=f"ACC_BOUNDARY_SRC_{i}"; account(a,"fan_in_source",st,label); transfer(a,col,100,date(2026,3,i))
    for i,amount in enumerate((6666.67,6666.67,6666.66),1):
        a=f"ACC_BOUNDARY_DEST_{i}"; account(a,"fan_out_destination",st,label); transfer(col,a,amount,date(2026,i+2,i),"wire")
    fan(*SCENARIOS[7][:2],SCENARIOS[7][2],device="only")
    fan(*SCENARIOS[8][:2],SCENARIOS[8][2],ns=18,nd=10,inbound=1200,outbound=2600)
    baseline=[f"ACC_B_{i}" for i in range(1,BASELINE_ACCOUNT_COUNT+1)]
    for a in baseline: account(a,"baseline","baseline_noise","Ordinary baseline account",date(2023,1,1),COHORT_BASELINE)
    for a in baseline: transfer(a,rng.choice([x for x in baseline if x!=a]),rng.uniform(50,350),policy.REFERENCE_DATE-timedelta(days=rng.randint(1,300)))
    return {"accounts":pd.DataFrame(accounts),"devices":pd.DataFrame(devices),"device_links":pd.DataFrame(links),"transfers":pd.DataFrame(transfers),"sessions":pd.DataFrame(sessions)}

def generate_dataset_scaled(seed=policy.DEFAULT_SEED,scale_factor=1):
    if scale_factor<1: raise ValueError("scale_factor must be >= 1")
    return generate_dataset(seed)
