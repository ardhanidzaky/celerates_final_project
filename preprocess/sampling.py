import random

def sample_rows(df, subset_size=1000, num_subsets=8):
    if len(df) < subset_size * num_subsets:
        raise ValueError("The dataset is too small to create the required number of subsets with the given subset size.")
    
    indices = list(df.index)
    random.shuffle(indices)
    
    # Split the indices into the desired number of subsets
    subsets = []
    for i in range(num_subsets):
        if len(indices) < subset_size:
            break
        subset_indices = indices[:subset_size]
        indices = indices[subset_size:]
        subsets.append(df.loc[subset_indices])
    
    return subsets

data = pd.read_csv('master_data.csv')
subsets = sample_rows(data)