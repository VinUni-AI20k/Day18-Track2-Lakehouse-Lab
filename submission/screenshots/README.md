# Screenshot checklist

The rubric needs at least one lightweight-path evidence image. From the repo
root, open `submission/screenshots/` and capture one terminal screenshot that
shows both commands and their output:

```powershell
tree _lakehouse\scratch\users_delta /F
Get-Content _lakehouse\scratch\users_delta\_delta_log\00000000000000000000.json
```

The first command proves the Delta table and `_delta_log` exist; the second
shows an actual commit JSON. A second useful image is the final `8/8 passed`
output from `scripts/run_all.py`, but it is optional because the executed
`.ipynb` files already contain the outputs.
