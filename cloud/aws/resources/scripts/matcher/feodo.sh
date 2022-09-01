vast matcher start --mode=exact --match-types=addr feodo

curl -sSL "https://feodotracker.abuse.ch/downloads/ipblocklist.csv" |
    tr -d '\015' | grep -v '^#' |
    vast matcher import --type=feodo.blocklist csv feodo

echo -n "imported lines: "
vast matcher save feodo | wc -l
