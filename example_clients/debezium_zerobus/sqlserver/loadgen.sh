#!/usr/bin/env bash
# Generates a steady trickle of change so there is always something to watch
# arrive in Delta. Every round inserts and updates an order; every third round
# touches a customer; every fifth deletes a delivered order, so all four CDC
# operations show up within about fifteen seconds of starting.
#
# Deliberately not `set -e`: a transient connection error should log and carry on
# rather than kill the container and lose the sequence.

set -uo pipefail

SQLCMD=/opt/mssql-tools18/bin/sqlcmd
DB="${MSSQL_DATABASE:-inventory}"
INTERVAL="${LOADGEN_INTERVAL_SECONDS:-3}"

CITIES=(Chicago Lagos Bengaluru Rotterdam "São Paulo" Osaka Toronto Manchester)
NAMES=("Dana Whitfield" "Marcus Oyelaran" "Priya Raghunathan" "Tomas Berglund"
       "Aiko Watanabe" "Rafael Duarte" "Nadia Haddad" "Ines Sorensen")
STATUSES=(NEW PAID SHIPPED DELIVERED CANCELLED)

run_sql() {
    "$SQLCMD" -S sqlserver -U sa -P "$MSSQL_SA_PASSWORD" -C -b -d "$DB" -Q "$1" \
        >/dev/null 2>&1 || echo "  statement failed, continuing"
}

# Indirect array expansion rather than a nameref, so this works on older bash too.
pick() {
    local name="$1[@]"
    local -a items=("${!name}")
    echo "${items[RANDOM % ${#items[@]}]}"
}

echo "load generator starting against $DB, one round every ${INTERVAL}s"

n=0
while true; do
    n=$((n + 1))

    status="$(pick STATUSES)"
    amount="$(( (RANDOM % 90000) + 1000 ))"
    run_sql "INSERT INTO dbo.orders (customer_id, order_status, amount, order_date)
             VALUES ((SELECT TOP 1 customer_id FROM dbo.customers ORDER BY NEWID()),
                     '${status}', ${amount}.$(printf '%02d' $((RANDOM % 100))),
                     CAST(SYSUTCDATETIME() AS DATE));"

    run_sql "UPDATE TOP (1) dbo.orders
             SET order_status = 'SHIPPED', updated_at = SYSUTCDATETIME()
             WHERE order_status IN ('NEW', 'PAID');"

    if (( n % 3 == 0 )); then
        name="$(pick NAMES)"
        city="$(pick CITIES)"
        slug="$(printf '%05d' "$n")"
        run_sql "INSERT INTO dbo.customers (full_name, email, city)
                 VALUES (N'${name}', 'user${slug}@example.com', N'${city}');"

        run_sql "UPDATE TOP (1) dbo.customers
                 SET city = N'${city}', updated_at = SYSUTCDATETIME()
                 WHERE customer_id % 2 = 0;"
    fi

    if (( n % 5 == 0 )); then
        run_sql "DELETE TOP (1) FROM dbo.orders WHERE order_status = 'DELIVERED';"
    fi

    if (( n % 20 == 0 )); then
        echo "round ${n} done"
    fi

    sleep "$INTERVAL"
done
