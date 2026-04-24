' pm2_resurrect_hidden.vbs
' Khoi dong lai cac PM2 process ma khong hien thi cua so CMD.
Set WshShell = CreateObject("WScript.Shell")
WshShell.Run "cmd /c pm2 resurrect", 0, False
Set WshShell = Nothing
