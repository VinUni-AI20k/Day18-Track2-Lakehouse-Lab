# Lightweight Evidence Screenshot

`terminal_evidence.txt` is a real transcript captured from the generated local
lakehouse. It is not presented as a screenshot.

To create the required genuine screenshot, open PowerShell at the repository
root and run:

```powershell
tree _lakehouse /A
tree _lakehouse\scratch\users_delta /F /A
$commit = Get-ChildItem _lakehouse\scratch\users_delta\_delta_log\*.json |
  Sort-Object Name | Select-Object -First 1
Get-Content -LiteralPath $commit.FullName
```

Capture the terminal showing the local layout, `_delta_log` JSON filenames,
and the real commit contents. Save the image in this directory without
removing `terminal_evidence.txt`.
