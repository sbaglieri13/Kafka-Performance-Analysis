import numpy as np

def tukey_iqr_filter(values: list | np.ndarray) -> tuple[np.ndarray, np.ndarray]:
    """
    Rimuove outlier con metodo Tukey IQR.
    Soglia: [Q1 - 1.5*IQR, Q3 + 1.5*IQR]

    Returns:
        filtered: array con soli valori non-outlier
        mask: maschera booleana applicata all'array originale
    """
    arr = np.array(values, dtype=float)
    
    # Calcola Q1, Q3 e IQR
    Q1, Q3 = np.percentile(arr, [25, 75])
    IQR = Q3 - Q1
    
    # Calcola i limiti
    lower = Q1 - 1.5 * IQR
    upper = Q3 + 1.5 * IQR
    
    # Crea una maschera per i valori che rientrano nei limiti
    mask = (arr >= lower) & (arr <= upper)
    
    return arr[mask], mask
