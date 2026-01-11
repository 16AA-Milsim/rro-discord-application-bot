Enable logging for the Discourse → Discord relay service (NSSM)

Service name:
  16AA RRO Applications Bot

NSSM path:
  F:\16AA\zOther\nssm-2.24\win64\nssm.exe

1) Open PowerShell as Administrator.

2) Stop the service:
  net stop "16AA RRO Applications Bot"

3) Enable log files:
  F:\16AA\zOther\nssm-2.24\win64\nssm.exe set "16AA RRO Applications Bot" AppStdout "F:\16AA\zOther\rro_discord_application_bot\logs\stdout.log"
  F:\16AA\zOther\nssm-2.24\win64\nssm.exe set "16AA RRO Applications Bot" AppStderr "F:\16AA\zOther\rro_discord_application_bot\logs\stderr.log"

4) (Optional) Enable rotation (10 MB, online):
  F:\16AA\zOther\nssm-2.24\win64\nssm.exe set "16AA RRO Applications Bot" AppRotateFiles 1
  F:\16AA\zOther\nssm-2.24\win64\nssm.exe set "16AA RRO Applications Bot" AppRotateOnline 1
  F:\16AA\zOther\nssm-2.24\win64\nssm.exe set "16AA RRO Applications Bot" AppRotateBytes 10485760

5) Start the service:
  net start "16AA RRO Applications Bot"

Disable logging again (to prevent disk growth):
  net stop "16AA RRO Applications Bot"
  F:\16AA\zOther\nssm-2.24\win64\nssm.exe reset "16AA RRO Applications Bot" AppStdout
  F:\16AA\zOther\nssm-2.24\win64\nssm.exe reset "16AA RRO Applications Bot" AppStderr
  net start "16AA RRO Applications Bot"
"@ | Set-Content -Path $readmePath -Encoding UTF8
