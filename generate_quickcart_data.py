import argparse
import json
import os
import random
import string
from datetime import datetime, timedelta
from uuid import uuid4

# -------------------- Helpers --------------------

def rand_choice_weighted(pairs):
    total = sum(w for _, w in pairs)
    r = random.uniform(0, total)
    upto = 0
    for v, w in pairs:
        upto += w
        if upto >= r:
            return v
    return pairs[-1][0]

def iso(dt: datetime) -> str:
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")

def format_amount_messy(total_cents: int):
    mode = rand_choice_weighted([
        ("usd_symbol", 0.45), ("int_cents", 0.35),
        ("plain_string", 0.10), ("missing", 0.07), ("empty", 0.03)
    ])
    if mode == "missing": return None
    if mode == "empty": return ""
    if mode == "int_cents": return total_cents
    dollars = total_cents / 100.0
    if mode == "plain_string": return f"{dollars:.2f}"
    prefix = rand_choice_weighted([("$", 0.85), ("USD ", 0.10), ("$ ", 0.05)])
    return f"{prefix}{dollars:.2f}"

def random_email():
    user = "".join(random.choices(string.ascii_lowercase + string.digits, k=random.randint(6,12)))
    dom = random.choice(["gmail.com","yahoo.com","outlook.com","quickcart.test","example.com"])
    return f"{user}@{dom}"

def provider_ref():
    return "prov_" + uuid4().hex[:18]

def sql_escape(s: str) -> str:
    return s.replace("'", "''")

# -------------------- Data Generation --------------------

def generate(args):
    random.seed(args.seed)
    os.makedirs(args.outdir, exist_ok=True)

    start_dt = datetime.utcnow() - timedelta(days=args.days)
    end_dt = datetime.utcnow()

    order_rows, payment_rows, bank_rows, log_events = [], [], [], []

    # ---- Generate Orders ----
    order_ids = []
    for _ in range(args.orders):
        oid = f"ord_{uuid4().hex[:16]}"
        order_ids.append(oid)
        created_at = start_dt + timedelta(seconds=random.randint(0, int((end_dt-start_dt).total_seconds())))
        customer_id = f"cus_{uuid4().hex[:12]}"
        email = random_email()
        subtotal = random.randint(500,25000)
        shipping = random.choice([0,300,500,800,1200])
        tax = int(subtotal*random.choice([0.0,0.03,0.05,0.075,0.1]))
        total_cents = subtotal + shipping + tax
        is_test = 1 if random.random()<args.test_rate else 0
        if random.random()<0.35 and is_test==1:
            email = f"test_{email}"
        order_rows.append({
            "order_id": oid,
            "customer_id": customer_id,
            "customer_email": email,
            "order_total_cents": total_cents,
            "currency": "USD",
            "is_test": is_test,
            "created_at": iso(created_at)
        })

    # ---- Generate Payments ----
    payment_ids_by_order = {}
    all_payment_ids = []
    for oid in order_ids:
        attempts = rand_choice_weighted([(1,0.72),(2,0.20),(3,0.06),(4,0.02)])
        payment_ids_by_order[oid] = []
        order_total = next(r["order_total_cents"] for r in order_rows if r["order_id"]==oid)
        created_at = datetime.strptime(next(r["created_at"] for r in order_rows if r["order_id"]==oid),"%Y-%m-%dT%H:%M:%SZ")
        for a in range(attempts):
            pid = f"pay_{uuid4().hex[:16]}"
            payment_ids_by_order[oid].append(pid)
            all_payment_ids.append(pid)
            attempted_at = created_at + timedelta(minutes=random.randint(1,240), seconds=random.randint(0,59))
            status = rand_choice_weighted([("FAILED",0.18),("PENDING",0.07),("SUCCESS",0.75)])
            if a>0 and any(p.get("status")=="SUCCESS" for p in payment_rows if p.get("order_id")==oid):
                status = rand_choice_weighted([("FAILED",0.70),("SUCCESS",0.20),("PENDING",0.10)])
            payment_rows.append({
                "payment_id": pid,
                "order_id": oid,
                "attempt_no": a+1,
                "provider": random.choice(["stripe","paypal","flutterwave"]),
                "provider_ref": provider_ref(),
                "status": status,
                "amount_cents": order_total,
                "attempted_at": iso(attempted_at)
            })

    # ---- Orphan Payments ----
    orphan_count = int(args.orphan_payment_rate*len(payment_rows))
    for _ in range(orphan_count):
        pid = f"pay_{uuid4().hex[:16]}"
        all_payment_ids.append(pid)
        attempted_at = start_dt + timedelta(seconds=random.randint(0,int((end_dt-start_dt).total_seconds())))
        amount_cents = random.randint(500,30000)
        status = rand_choice_weighted([("SUCCESS",0.65),("FAILED",0.25),("PENDING",0.10)])
        payment_rows.append({
            "payment_id": pid,
            "order_id": None,
            "attempt_no": 1,
            "provider": random.choice(["stripe","paypal","flutterwave"]),
            "provider_ref": provider_ref(),
            "status": status,
            "amount_cents": amount_cents,
            "attempted_at": iso(attempted_at)
        })

    # ---- Bank Settlements ----
    success_payments = [p for p in payment_rows if p["status"]=="SUCCESS"]
    sample_size = min(len(success_payments), args.bank_rows)
    bank_sample = random.sample(success_payments, sample_size)
    for p in bank_sample:
        settled_at = datetime.strptime(p["attempted_at"],"%Y-%m-%dT%H:%M:%SZ")+timedelta(hours=random.randint(1,72))
        amt = p["amount_cents"]
        if random.random()<args.partial_settlement_rate: amt = int(amt*random.choice([0.5,0.8,0.9]))
        bank_rows.append({
            "settlement_id": f"set_{uuid4().hex[:16]}",
            "payment_id": p["payment_id"] if random.random()>args.bank_missing_payment_id_rate else None,
            "provider_ref": p["provider_ref"] if random.random()>args.bank_missing_provider_ref_rate else None,
            "status": "SETTLED",
            "settled_amount_cents": amt,
            "currency": "USD",
            "settled_at": iso(settled_at)
        })
        if random.random()<args.bank_duplicate_rate:
            dup = dict(bank_rows[-1])
            dup["settlement_id"] = f"set_{uuid4().hex[:16]}"
            bank_rows.append(dup)

    # ---- Raw JSON Logs ----
    for p in payment_rows:
        event_time = datetime.strptime(p["attempted_at"],"%Y-%m-%dT%H:%M:%SZ")
        log_events.append({
            "event":{"id":f"evt_{uuid4().hex[:18]}","type":rand_choice_weighted([("payment_attempted",0.45),("payment_succeeded",0.40),("payment_failed",0.15)]),"ts":iso(event_time),"source":rand_choice_weighted([("web",0.55),("mobile",0.35),("internal",0.10)])},
            "entity":{"order":{"id":p["order_id"]},"payment":{"id":p["payment_id"],"provider_ref":p["provider_ref"],"provider":p["provider"]},"customer":{"email":random_email()}},
            "payload":{"Amount":format_amount_messy(p["amount_cents"]),"currency":"USD","status":p["status"],"flags":None,"metadata":{"ip":".".join(str(random.randint(1,254)) for _ in range(4)),"user_agent":random.choice(["Chrome","Safari","Firefox","Edge","MobileApp"])}}})

    # ---- Write files ----
    os.makedirs(args.outdir, exist_ok=True)

    # JSONL
    raw_path = os.path.join(args.outdir,"raw_data.jsonl")
    with open(raw_path,"w",encoding="utf-8") as f:
        for ev in log_events:
            f.write(json.dumps(ev)+"\n")

    # SQL: orders
    with open(os.path.join(args.outdir,"seed_orders.sql"),"w",encoding="utf-8") as f:
        f.write("-- seed_orders.sql\n")
        for r in order_rows:
            f.write(f"INSERT INTO orders (order_id, customer_id, customer_email, order_total_cents, currency, is_test, created_at) VALUES ('{r['order_id']}', '{r['customer_id']}', '{sql_escape(r['customer_email'])}', {r['order_total_cents']}, '{r['currency']}', {r['is_test']}, '{r['created_at']}');\n")

    # SQL: payments
    with open(os.path.join(args.outdir,"seed_payments.sql"),"w",encoding="utf-8") as f:
        f.write("-- seed_payments.sql\n")
        for r in payment_rows:
            order_id_sql = "NULL" if r["order_id"] is None else f"'{r['order_id']}'"
            f.write(f"INSERT INTO payments (payment_id, order_id, attempt_no, provider, provider_ref, status, amount_cents, attempted_at) VALUES ('{r['payment_id']}', {order_id_sql}, {r['attempt_no']}, '{r['provider']}', '{r['provider_ref']}', '{r['status']}', {r['amount_cents']}, '{r['attempted_at']}');\n")

    # SQL: bank settlements
    with open(os.path.join(args.outdir,"seed_bank_settlements.sql"),"w",encoding="utf-8") as f:
        f.write("-- seed_bank_settlements.sql\n")
        for r in bank_rows:
            pid_sql = "NULL" if r["payment_id"] is None else f"'{r['payment_id']}'"
            pref_sql = "NULL" if r["provider_ref"] is None else f"'{r['provider_ref']}'"
            f.write(f"INSERT INTO bank_settlements (settlement_id, payment_id, provider_ref, status, settled_amount_cents, currency, settled_at) VALUES ('{r['settlement_id']}', {pid_sql}, {pref_sql}, '{r['status']}', {r['settled_amount_cents']}, '{r['currency']}', '{r['settled_at']}');\n")

    print(" All files generated successfully in:", args.outdir)
    print(f"  - {len(order_rows)} orders")
    print(f"  - {len(payment_rows)} payments")
    print(f"  - {len(bank_rows)} bank settlements")
    print(f"  - {len(log_events)} raw events")

# -------------------- Main --------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--outdir", default="quickcart_data")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--orders", type=int, default=1000)
    ap.add_argument("--bank-rows", type=int, default=1000)
    ap.add_argument("--days", type=int, default=30)
    ap.add_argument("--test-rate", type=float, default=0.06)
    ap.add_argument("--orphan-payment-rate", type=float, default=0.05)
    ap.add_argument("--partial-settlement-rate", type=float, default=0.03)
    ap.add_argument("--bank-duplicate-rate", type=float, default=0.02)
    ap.add_argument("--bank-missing-payment-id-rate", type=float, default=0.03)
    ap.add_argument("--bank-missing-provider-ref-rate", type=float, default=0.02)
    ap.add_argument("--log-missing-order-id-rate", type=float, default=0.04)
    ap.add_argument("--log-noise-rate", type=float, default=0.03)
    args = ap.parse_args()
    generate(args)

if __name__ == "__main__":
    main()
