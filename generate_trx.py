import random
import json
from datetime import datetime, timedelta
import uuid

NUM_ROWS = 10000
OUTPUT_FILE = "./data/input/sample_payments.json"

GAMES = [
    "funny-game", "cool-game", "extra-game", "space-explorer", "puzzle-master",
    "galactic-quest", "mystic-realms", "neon-rush", "dragon-voyage", "cosmic-puzzler",
    "shadow-hunt", "starforge-adventure", "pixel-palace", "time-twister", "orbit-odyssey"
]

GAME_WEIGHTS = [20, 15, 10, 8, 7, 5, 5, 4, 4, 3, 3, 2, 2, 1, 1]
CURRENCIES = ["EUR", "USD", "GBP", "CAD"]
STATUSES = ["successfull", "error"]
STATUS_WEIGHTS = [0.9, 0.1]
PRICE_RANGES = {
    "EUR": (0.49, 3.49),
    "USD": (0.54, 3.99),
    "GBP": (0.45, 2.99),
    "CAD": (0.60, 4.49)
}
START_DATE = datetime(2025, 6, 1)
END_DATE = datetime(2025, 6, 12)

def generate_random_transaction(payment_date=None):
    transaction_id = f"TRX{uuid.uuid4().hex[:10].upper()}"
    if payment_date is None:
        days_range = (END_DATE - START_DATE).days
        random_days = random.randint(0, days_range)
        payment_date = (START_DATE + timedelta(days=random_days)).strftime("%Y-%m-%d")
    game = random.choices(GAMES, weights=GAME_WEIGHTS, k=1)[0]
    currency = random.choice(CURRENCIES)
    status = random.choices(STATUSES, weights=STATUS_WEIGHTS, k=1)[0]
    min_price, max_price = PRICE_RANGES[currency]
    price = round(random.uniform(min_price, max_price), 2)
    return {
        "transaction_id": transaction_id,
        "payment_date": payment_date,
        "game": game,
        "currency": currency,
        "price": price,
        "status": status
    }

transactions = [generate_random_transaction() for _ in range(NUM_ROWS)]

# Add extra transactions on random days to create uneven distribution
days_range = (END_DATE - START_DATE).days
boost_days = random.sample(range(days_range + 1), k=3)  # Pick 3 random days
for day in boost_days:
    boost_date = (START_DATE + timedelta(days=day)).strftime("%Y-%m-%d")
    extra_transactions = random.randint(100, 500)  # Add 100-500 extra transactions
    transactions.extend([generate_random_transaction(boost_date) for _ in range(extra_transactions)])

with open(OUTPUT_FILE, "w") as f:
    json.dump(transactions, f, indent=2)

print(f"Generated {len(transactions)} transactions and saved to {OUTPUT_FILE}")