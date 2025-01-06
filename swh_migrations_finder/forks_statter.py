import matplotlib.pyplot as plt
import numpy as np
from typing import List

def plot_fork_stats(all_forks_status : List[List[int]], output_path="plot.png"):
    """
    all_forks_status : List[List[int]] : The list of fork status for each Inria source

    The fork status is represented as follows:
    0 : explicit_forks
    1 : explicit_non_forks
    2 : dont_know
    """
    all_forks_status = [np.array(forks_status) for forks_status in all_forks_status]

    # Get the number of each fork status for each Inria source
    stats = np.array([
        [np.sum(forks==0),np.sum(forks==1),np.sum(forks==2)]
        for forks in all_forks_status])
    
    # Sort by explicit_forks + dont_know
    stats = stats[np.argsort(stats[:,1] + stats[:,2])] 
    explicit_forks = stats[:,0]
    explicit_non_forks = stats[:,1]
    dont_know = stats[:,2]
    x = np.arange(len(stats))

    # Plot the stacked bars
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(x, explicit_non_forks, label="explicit_non_forks", color="#00e673")
    ax.bar(x, dont_know, bottom=explicit_non_forks, label="dont_know", color="#FFA500")
    ax.bar(x, explicit_forks, bottom=explicit_non_forks+dont_know, label="explicit_forks", color="#FFDAB9")

    top_limit = stats[-1][1] + stats[-1][2]
    ax.set_ylim(0, 1.2 * top_limit)

    # Add labels and legend
    ax.set_xlabel("Inria sources")
    ax.set_ylabel("Number of Migrations")
    ax.set_title("Number of migration for each Inria source after github explicit fork elagation")
    ax.legend()
    ax.set_xticks([])
    
    # Adjust layout and show plot
    plt.tight_layout()
    plt.savefig(output_path)
    plt.close()

    print('saved graph to', output_path)
