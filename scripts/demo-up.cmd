@echo off
rem One click: a Windows Terminal window with the six demo panes, each in WSL, in the repo, venv active.
rem   1 brokers (up.sh, agent mesh, model warm-up)  |  2 generator  |  3 agency feeds
rem   spare shell                                   |  4 alerting   |  5 fusion
rem Panes 2-5 wait until pane 1 has bootstrapped the brokers, then start their service.
rem Closing the window stops the Python services; the brokers and the agent mesh keep running in Docker.
set P=/mnt/c/demos/Solace/edge-cop-solace/scripts/pane.sh
rem stop any demo service left over from an earlier window, so every pane starts clean
wsl.exe -e bash -lc "pkill -f 'python3 (sim|services)/' ; true"
wt -w cop-demo new-tab --title "1 brokers" wsl.exe -e bash -lc "%P% brokers" ^
 ; split-pane -V -s 0.5 --title "2 generator" wsl.exe -e bash -lc "%P% generator" ^
 ; split-pane -V -s 0.5 --title "3 feeds" wsl.exe -e bash -lc "%P% feeds" ^
 ; move-focus left ; move-focus left ^
 ; split-pane -H --title "shell" wsl.exe -e bash -lc "%P% shell" ^
 ; move-focus right ; split-pane -H --title "4 alerting" wsl.exe -e bash -lc "%P% alerting" ^
 ; move-focus right ; split-pane -H --title "5 fusion" wsl.exe -e bash -lc "%P% fusion"
