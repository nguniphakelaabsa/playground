#!/bin/bash

# ============================================
# SMSC Messages Dispatched Report Generator
# Includes: status (Completed, Failed, etc.), retry indication, channel summary
#
# Usage: ./reports-messages-sent.sh
# Override MongoDB: MONGO_URI="..." ./reports-messages-sent.sh
#
# Recommended indexes for performance:
#   db['message-repo'].createIndex({ timestamp: 1 })
#   db['transaction-log'].createIndex({ txid: 1 })
#   db['transaction-log'].createIndex({ status_timestamp: 1 })
# ============================================

set -e
trap 'echo "❌ Error occurred during execution. Exiting..."; exit 1;' ERR

# Load environment variables from scripts/.env (can override with ENV_FILE=...)
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
ENV_FILE="${ENV_FILE:-$SCRIPT_DIR/.env}"
if [ ! -f "$ENV_FILE" ]; then
  echo "❌ Required env file not found: $ENV_FILE"
  echo "   Create it (e.g. $SCRIPT_DIR/.env) and define required variables."
  exit 1
fi

set -o allexport
# shellcheck disable=SC1090
source "$ENV_FILE"
set +o allexport

require_var() {
  local name="$1"
  if [ -z "${!name:-}" ]; then
    echo "❌ Missing required env var: $name (define it in $ENV_FILE)"
    exit 1
  fi
}

# Require all configuration to be provided via .env (no defaults in script)
require_var "MONGO_URI"
require_var "DB_NAME"
require_var "COLL_LOG"
require_var "COLL_REPO"
require_var "START_DATE"
require_var "END_DATE"

# Reporting period is configured via START_DATE / END_DATE in .env

# Logging setup
LOG_FILE="smsc_report_log_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG_FILE") 2>&1

# Portable: convert date to epoch milliseconds (macOS and Linux)
date_to_epoch_ms() {
  local d="$1"
  if date -j -f "%Y-%m-%d %H:%M:%S" "$d" +%s >/dev/null 2>&1; then
    echo $(($(date -j -f "%Y-%m-%d %H:%M:%S" "$d" +%s) * 1000))
  else
    echo $(($(date -u -d "$d" +%s) * 1000))
  fi
}

TS_START=$(date_to_epoch_ms "${START_DATE} 00:00:00")
# Exclusive upper bound: first moment of the day after END_DATE
NEXT_DAY=$(date -j -v+1d -f "%Y-%m-%d" "$END_DATE" +%Y-%m-%d 2>/dev/null || date -d "$END_DATE + 1 day" +%Y-%m-%d 2>/dev/null)
TS_END_EXCL=$(date_to_epoch_ms "${NEXT_DAY} 00:00:00")

echo "======================================"
echo "SMSC Messages Dispatched Report"
echo "Period: $START_DATE to $END_DATE (inclusive)"
echo "Database: $DB_NAME"
echo "======================================"

# Step 1: Drop temp collections
echo "Preparing collections..."
mongosh "$MONGO_URI" --quiet --eval "
  db.report_txids.drop();
  db.tx_status_lookup.drop();
  db.message_results_final.drop();
"

# Step 2: Get distinct txids from message-repo in date range (single query)
echo "Step 1/3: Finding transaction IDs in period..."
mongosh "$MONGO_URI" --quiet --eval "
  db['$COLL_REPO'].aggregate([
    { \$match: { timestamp: { \$gte: NumberLong($TS_START), \$lt: NumberLong($TS_END_EXCL) } } },
    { \$group: { _id: '\$txid' } },
    { \$out: 'report_txids' }
  ]);
  print('TxIDs in scope: ' + db.report_txids.countDocuments({}));
"

# Step 3: Build status + retry lookup from transaction-log (single aggregation)
echo "Step 2/3: Building status and retry lookup from transaction-log..."
mongosh "$MONGO_URI" --quiet --eval "
  db['$COLL_LOG'].aggregate([
    { \$lookup: { from: 'report_txids', localField: 'txid', foreignField: '_id', as: 'in_scope' } },
    { \$match: { in_scope: { \$ne: [] } } },
    { \$sort: { status_timestamp: 1 } },
    { \$group: {
        _id: '\$txid',
        entries: { \$push: { status: '\$status', status_timestamp: '\$status_timestamp' } }
      }
    },
    { \$project: {
        txid: '\$_id',
        last_status: { \$arrayElemAt: [ '\$entries', -1 ] },
        attempt_count: {
          \$size: {
            \$filter: {
              input: '\$entries',
              as: 'e',
              cond: { \$in: [ '\$\$e.status', [ 'Queued', 'Started' ] ] }
            }
          }
        }
      }
    },
    { \$project: {
        txid: 1,
        status: '\$last_status.status',
        retried: { \$cond: [ { \$gt: [ '\$attempt_count', 2 ] }, 'Yes', 'No' ] }
      }
    },
    { \$out: 'tx_status_lookup' }
  ]);
  print('Status lookup rows: ' + db.tx_status_lookup.countDocuments({}));
"

# Step 4: Join message-repo with status, write final results (single aggregation)
echo "Step 3/3: Building final message list with status and retry..."
mongosh "$MONGO_URI" --quiet --eval "
  db['$COLL_REPO'].aggregate([
    { \$match: { timestamp: { \$gte: NumberLong($TS_START), \$lt: NumberLong($TS_END_EXCL) } } },
    { \$lookup: { from: 'tx_status_lookup', localField: 'txid', foreignField: 'txid', as: 'tx_status' } },
    { \$unwind: { path: '\$tx_status', preserveNullAndEmptyArrays: true } },
    { \$project: {
        txid: 1,
        channel: 1,
        recipient: { \$toString: '\$recipient' },
        value: 1,
        timestamp_readable: {
          \$dateToString: {
            format: '%Y-%m-%d %H:%M:%S',
            date: { \$toDate: '\$timestamp' },
            timezone: 'UTC'
          }
        },
        status: { \$ifNull: [ '\$tx_status.status', 'Unknown' ] },
        retried: { \$ifNull: [ '\$tx_status.retried', 'No' ] },
        _id: 0
      }
    },
    { \$out: 'message_results_final' }
  ]);
  print('Total messages in report: ' + db.message_results_final.countDocuments({}));
"

# Step 5: Export to CSV
OUTPUT_FILE="SMSC_Report_${START_DATE}_to_${END_DATE}_$(date +%Y%m%d%H%M%S).csv"
echo "Exporting CSV..."
mongoexport \
  --uri="$MONGO_URI" \
  --collection="message_results_final" \
  --type=csv \
  --fields txid,channel,recipient,value,timestamp_readable,status,retried \
  --out "$OUTPUT_FILE"

# Step 6: Channel summary (Airtel, MTN, iTouch, Marketing)
echo ""
echo "======================================"
echo "Channel summary (all messages in period)"
echo "======================================"
SUMMARY_FILE="SMSC_Summary_${START_DATE}_to_${END_DATE}_$(date +%Y%m%d%H%M%S).txt"
mongosh "$MONGO_URI" --quiet --eval "
  var channels = [ 'Airtel', 'MTN', 'iTouch', 'Marketing' ];
  var summary = [];
  var total = 0;
  channels.forEach(function(ch) {
    var count = db.message_results_final.countDocuments({ channel: ch });
    summary.push('Total ' + ch + ' Messages: ' + count);
    total += count;
  });
  summary.push('Total All Channels: ' + total);
  summary.push('');
  summary.push('(Channels not in the list above are included in CSV under their channel name.)');
  print(summary.join('\n'));
" | tee "$SUMMARY_FILE"

# Step 7: Compress for sharing
ZIP_FILE="${OUTPUT_FILE%.csv}.zip"
zip -q "$ZIP_FILE" "$OUTPUT_FILE" "$SUMMARY_FILE"

# Completion
echo ""
echo "======================================"
echo "SMSC Report generated successfully!"
echo "  CSV    : $OUTPUT_FILE"
echo "  Summary: $SUMMARY_FILE"
echo "  ZIP    : $ZIP_FILE"
echo "  Log    : $LOG_FILE"
echo "======================================"
echo "Process completed successfully."
