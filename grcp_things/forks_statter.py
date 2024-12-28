import matplotlib.pyplot as plt
import numpy as np

if __name__ == "__main__":
    stats = []
    curr_stats = [0,0,0]
    with open('migration_fork_check.txt') as f:
        line = f.readline()
        while line:
            if line[0] != ' ':
                if curr_stats != [0,0,0]:
                    stats.append(curr_stats)
                curr_stats = [0,0,0]
            else:
                curr_stats[int(line[31])] += 1
            line = f.readline()
    stats.append(curr_stats)
    stats = np.array(stats)
    stats = stats[np.argsort(stats[:,1] + stats[:,2])] # sort by explicit_forks + dont_know
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
    plt.savefig("plot.png")
    plt.close()