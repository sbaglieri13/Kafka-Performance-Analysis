import numpy as np
from scipy import stats

def compute_ci(values: list | np.ndarray, confidence: float = 0.95) -> dict:
    """
    Calcola media e intervallo di confidenza con t-distribution.

    Returns:
        dict con chiavi: mean, ci_lower, ci_upper, ci_half, std, n
    """
    arr = np.array(values, dtype=float)
    n = len(arr)

    if n == 0:
        return {"mean": None, "ci_lower": None,
                "ci_upper": None, "ci_half": None,
                "std": None, "n": 0}
    if n == 1:
        return {"mean": float(arr[0]), "ci_lower": float(arr[0]),
                "ci_upper": float(arr[0]), "ci_half": 0.0,
                "std": 0.0, "n": 1}

    mean = np.mean(arr) # media campione
    se = stats.sem(arr) # errore standard della media (std campione / sqrt(n))
    t_crit = stats.t.ppf((1 + confidence) / 2, df=n - 1) # t critico per intervallo di confidenza
    ci_half = t_crit * se # metà ampiezza dell'intervallo di confidenza

    return {
        "mean": round(float(mean), 4), # media campione arrotondata a 4 decimali
        "ci_lower": round(float(mean - ci_half), 4), # limite inferiore dell'intervallo di confidenza
        "ci_upper": round(float(mean + ci_half), 4), # limite superiore dell'intervallo di confidenza
        "ci_half": round(float(ci_half), 4), # metà ampiezza dell'intervallo di confidenza
        "std": round(float(np.std(arr, ddof=1)), 4), # deviazione standard campione
        "n": n # numero di campioni
    }
