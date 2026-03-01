import numpy as np
from scipy import stats

def compute_ci(values: list | np.ndarray, confidence: float = 0.95) -> dict:
    """
    Computes the mean and confidence interval using the t-distribution.

    Returns:
        dict with keys: mean, ci_lower, ci_upper, ci_half, std, n
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

    mean = np.mean(arr) # mean of the sample
    se = stats.sem(arr) # Standard error of the mean (sample std / sqrt(n))
    t_crit = stats.t.ppf((1 + confidence) / 2, df=n - 1) # Critical t-value for the confidence interval
    ci_half = t_crit * se # Half-width of the confidence interval

    return {
        "mean": round(float(mean), 4), # mean of the sample rounded to 4 decimal places
        "ci_lower": round(float(mean - ci_half), 4), # lower bound of confidence interval
        "ci_upper": round(float(mean + ci_half), 4), # upper bound of confidence interval
        "ci_half": round(float(ci_half), 4), # half-width of confidence interval
        "std": round(float(np.std(arr, ddof=1)), 4), # sample standard deviation
        "n": n # number of samples
    }
