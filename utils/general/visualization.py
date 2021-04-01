import os

import pandas as pd
from matplotlib import pyplot as plt



def time_series_plot(data,
                     x_label,
                     y_label,
                     plot_title,
                     save=False,
                     path='.',
                     file_name='ts_plot.png',
                     figsize=(21, 7),
                     xlabelrotation=0):
    x = [val[0] for val in data]
    y = [val[1] for val in data]
    fig, ax = plt.subplots(nrows=1, ncols=1, figsize=figsize)
    ax.set_xlabel(x_label, fontsize=12)
    ax.set_ylabel(y_label, fontsize=12)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    plt.plot_date(x=x, y=y, fmt="r-")
    plt.xticks(rotation=xlabelrotation)
    plt.title(plot_title)
    ax.grid(axis='y')
    if save:
        plt.savefig(os.path.join(path, file_name))
    plt.show()


def bar_plot_horizontal(data,
                        x_label,
                        y_label,
                        plot_title,
                        save=False,
                        path='.',
                        file_name='barh_plot.png',
                        figsize=(7, 21),
                        xlabelrotation=0):
    y = [val[0] for val in data]
    x = [val[1] for val in data]
    fig, ax = plt.subplots(nrows=1, ncols=1)
    ax.set_xlabel(x_label, fontsize=12)
    ax.set_ylabel(y_label, fontsize=12)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.tick_params(axis='x', labelrotation=xlabelrotation)
    pd.Series(x).plot(kind='barh', figsize=figsize, title=plot_title, ax=ax)
    ax.set_yticklabels(y)
    ax.grid(axis='x')

    for i, v in enumerate(x):
        ax.text(int(v) + 0.5, i - 0.25, str(v), ha='left', va='bottom')
    plt.tight_layout()
    if save:
        plt.savefig(os.path.join(path, file_name))
    plt.show()


def bar_plot(data,
             x_label,
             y_label,
             plot_title,
             save=False,
             path='.',
             file_name='bar_plot.png',
             figsize=(21,7),
             xlabelrotation=90):
    x = [v[0] for v in data]
    y = [v[1] for v in data]

    fig, ax = plt.subplots(nrows=1, ncols=1)
    ax.set_xlabel(x_label, fontsize=12)
    ax.set_ylabel(y_label, fontsize=12)
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.tick_params(axis='x', labelrotation=xlabelrotation)
    pd.Series(y).plot(kind='bar', figsize=figsize, title=plot_title, ax=ax)
    ax.set_xticklabels(x)
    ax.grid(axis='y')

    for idx, rect in enumerate(ax.patches):
        _format = "{:,d}"
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width() / 2,
                height + 5,
                round(height, 2),
                ha='center',
                va='bottom',
                rotation=0,
                fontsize=12)

    plt.tight_layout()
    if save:
        plt.savefig(os.path.join(path, file_name))
    plt.show()