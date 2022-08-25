vast matcher start --mode=exact --match-types=addr pulsedive

curl -sSL "https://pulsedive.com/premium/?key=&header=true&fields=id,type,risk,threats,feeds,usersubmissions,riskfactors,reference&types=ip,ipv6&risk=unknown,none,low,medium,high,critical&period=all&retired=false" |
    vast matcher import --type=pulsedive csv pulsedive

echo -n "imported lines: "
vast matcher save pulsedive | wc -l
