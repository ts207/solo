# Detector Alias Policy

| Alias | Canonical Event | Scope | Planning Identity | Runtime Identity | Promotion Identity | Reason |
|---|---|---|---|---|---|---|
| ABSORPTION_EVENT | ABSORPTION_PROXY | load_time_compatibility | False | False | False | Legacy detector registration name retained for old artifacts. |
| BASIS_DISLOCATION | BASIS_DISLOC | load_time_compatibility | False | False | False | Historical spelling retained for old proposals and reports. |
| PRICE_DOWN_OI_DOWN | OI_SPIKE_NEGATIVE | runtime_identity | False | False | False |  |
| PRICE_UP_OI_DOWN | OI_SPIKE_NEGATIVE | runtime_identity | False | False | False |  |
| VOL_REGIME_SHIFT | VOL_REGIME_SHIFT_EVENT | load_time_compatibility | False | False | False | Historical event id retained for old detector registrations. |
