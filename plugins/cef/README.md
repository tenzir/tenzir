# CEF Plugin for VAST

This [reader plugin](https://docs.tenzir.com/vast/architecture/plugins#reader)
parse input in the [Common Event Format (CEF)][cef], a line-based ASCII format
representing security events.

Here is an example of CEF log line:

```
CEF:0|ArcSight|ArcSight|6.0.3.6664.0|agent:030|Agent [test] type [testalertng] started|Low|eventId=1 mrt=1396328238973 categorySignificance=/Normal categoryBehavior=/Execute/Start categoryDeviceGroup=/Application catdt=Security Mangement categoryOutcome=/Success categoryObject=/Host/Application/Service art=1396328241038 cat=/Agent/Started deviceSeverity=Warning rt=1396328238937 fileType=Agent cs2=<Resource ID\="3DxKlG0UBABCAA0cXXAZIwA\=\="/> c6a4=fe80:0:0:0:495d:cc3c:db1a:de71 cs2Label=Configuration Resource c6a4Label=Agent IPv6 Address ahost=SKEELES10 agt=888.99.100.1 agentZoneURI=/All Zones/ArcSight System/Private Address Space Zones/RFC1918: 888.99.0.0-888.200.255.255 av=6.0.3.6664.0 atz=Australia/Sydney aid=3DxKlG0UBABCAA0cXXAZIwA\=\= at=testalertng dvchost=SKEELES10 dvc=888.99.100.1 deviceZoneURI=/All Zones/ArcSight System/Private Address Space Zones/RFC1918:888.99.0.0-888.200.255.255 dtz=Australia/Sydney _cefVer=0.1
```

[cef]: https://kc.mcafee.com/resources/sites/MCAFEE/content/live/CORP_KNOWLEDGEBASE/78000/KB78712/en_US/CEF_White_Paper_20100722.pdf
