filepath = "project/events/families/regime.py"
with open(filepath, "r") as f:
    lines = f.readlines()

with open(filepath, "w") as f:
    for line in lines:
        if "'VOL_REGIME_SHIFT': VolRegimeShiftDetector," in line:
            continue
        f.write(line)
