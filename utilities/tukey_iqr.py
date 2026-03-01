import numpy as np

def tukey_iqr_filter(values: list | np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    Removes outliers using the Tukey IQR method.
    Threshold: [Q1 - 1.5*IQR, Q3 + 1.5*IQR]

    Returns:
        filtered: array containing only non-outlier values
        mask: boolean mask applied to the original array
    """
    arr = np.array(values, dtype=float)
    
    # Calculate Q1, Q3, and IQR
    Q1, Q3 = np.percentile(arr, [25, 75])
    IQR = Q3 - Q1
    
    # Calculate the bounds
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    
    # Create a mask for values that fall within the bounds
    mask = (arr >= lower) & (arr <= upper)
    
    return arr[mask], mask
