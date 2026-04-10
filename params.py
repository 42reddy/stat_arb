# Core parameters — tuned for 14.5d half-life
PARAMS = dict(

    T1 = 'HAL.NS',
    T2 = 'BDL.NS',
    # ── Windows ──
    slow_window    = 23,
    medium_window  = 34,
    fast_span      = 10,
    vol_window     = 20,

    # ── Asymmetric entry thresholds ──
    # Long spread  = Brent premium collapsed (historically rarer, faster)
    # Short spread = Brent premium extended  (more common, slower to revert)
    z_entry_long   = 1.7065,    # tighter — these moves are sharp, catch early
    z_entry_short  = 1.8698,    # wider  — premium extensions linger longer

    # ── Asymmetric exits ──
    z_exit_long    = 0.1105,    # exit long faster — mean reversion can overshoot
    z_exit_short   = 0.2443,    # exit short slower — premium tends to compress gradually

    # ── Asymmetric stops ──
    z_stop_long    = 2.8,    # wider stop long — dislocation events are violent
    z_stop_short   = 2.8,    # tighter stop short — extended premiums rarely blow out

    # ── Pyramid ──
    z_add          = 2.0,
    vol_cap        = 1.5,
    max_hold       = 25,

    # ── Kept for compatibility ──
    z_entry        = 1.25,   # fallback if features/signals not yet updated
    z_exit         = 0.30,
    z_stop         = 3.0,

    # ── Autocorrelation regime filter ──
    autocorr_window    = 20,
    autocorr_threshold = 0.1,
    ou_adapt_span      = 252,
)
