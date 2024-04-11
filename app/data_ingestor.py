import os
import json
import pandas as pd

class DataIngestor:
    def __init__(self, csv_path: str):
        self.data = pd.read_csv(csv_path)

        self.questions_best_is_min = [
            'Percent of adults aged 18 years and older who have an overweight classification',
            'Percent of adults aged 18 years and older who have obesity',
            'Percent of adults who engage in no leisure-time physical activity',
            'Percent of adults who report consuming fruit less than one time daily',
            'Percent of adults who report consuming vegetables less than one time daily'
        ]

        self.questions_best_is_max = [
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who achieve at least 150 minutes a week of moderate-intensity aerobic physical activity or 75 minutes a week of vigorous-intensity aerobic physical activity and engage in muscle-strengthening activities on 2 or more days a week',
            'Percent of adults who achieve at least 300 minutes a week of moderate-intensity aerobic physical activity or 150 minutes a week of vigorous-intensity aerobic activity (or an equivalent combination)',
            'Percent of adults who engage in muscle-strengthening activities on 2 or more days a week',
        ]
    
    def states_mean(self, question):
        samples = self.data.copy()
        samples = samples[samples['Question'] == question].loc[:, ('LocationDesc', 'Data_Value')]
        groups = samples.groupby(['LocationDesc']).mean()
        ordered = groups['Data_Value'].sort_values()

        return ordered.to_dict()

    def state_mean(self, question, state):
        samples = self.data.copy()
        samples = samples[(samples['LocationDesc'] == state) & (samples['Question'] == question)]
        mean = samples['Data_Value'].mean()

        return {state: mean}

    def best5(self, question):
        sortDirection = question in self.questions_best_is_min

        samples = self.data.copy()
        samples = samples[samples['Question'] == question].loc[:, ('LocationDesc', 'Data_Value')]
        groups = samples.groupby(['LocationDesc']).mean()
        ordered = groups['Data_Value'].sort_values(ascending=sortDirection)

        return ordered.head().to_dict()

    def worst5(self, question):
        sortDirection = question in self.questions_best_is_max

        samples = self.data.copy()
        samples = samples[samples['Question'] == question].loc[:, ('LocationDesc', 'Data_Value')]
        groups = samples.groupby(['LocationDesc']).mean()
        ordered = groups['Data_Value'].sort_values(ascending=sortDirection)

        return ordered.head().to_dict()

    def global_mean(self, question):
        samples = self.data.copy()
        samples = samples[samples['Question'] == question]
        mean = samples['Data_Value'].mean()

        return {"global_mean": mean}

    def diff_from_mean(self, question):
        samples = self.data.copy()
        samples = samples[samples['Question'] == question].loc[:, ('LocationDesc', 'Data_Value')]
        globalMean = samples['Data_Value'].mean()
        groups = samples.groupby(['LocationDesc']).mean()
        groups = groups.apply(lambda x: globalMean - x)
        ordered = groups['Data_Value'].sort_values(ascending=False)

        return ordered.to_dict()

    def state_diff_from_mean(self, question, state):
        samples = self.data.copy()
        samples = samples[samples['Question'] == question].loc[:, ('LocationDesc', 'Data_Value')]
        globalMean = samples['Data_Value'].mean()
        sample = samples[samples['LocationDesc'] == state]['Data_Value']
        value = globalMean - sample.mean()

        return {state: value}

    def mean_by_category(self, question):
        samples = self.data.copy()
        samples = samples[samples['Question'] == question].loc[:, ('LocationDesc', 'StratificationCategory1', 'Stratification1', 'Data_Value')]
        groups = samples.groupby(['LocationDesc', 'StratificationCategory1', 'Stratification1']).mean()
        groups['new'] = groups.apply(lambda x: f"('{x.name[0]}', '{x.name[1]}', '{x.name[2]}')", axis=1)
        groups = groups.set_index("new")['Data_Value']

        return groups.to_dict()

    def state_mean_by_category(self, question, state):
        samples = self.data.copy()
        samples = samples[(samples['Question'] == question) & (samples['LocationDesc'] == state)].loc[:, ('StratificationCategory1', 'Stratification1', 'Data_Value')]
        groups = samples.groupby(['StratificationCategory1', 'Stratification1']).mean()
        groups['new'] = groups.apply(lambda x: f"('{x.name[0]}', '{x.name[1]}')", axis=1)
        groups = groups.set_index("new")['Data_Value']

        return {state:groups.to_dict()}