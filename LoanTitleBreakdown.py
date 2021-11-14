#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from mrjob.job import MRJob
from mrjob.step import MRStep


class LoanTitleBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_title,
                   reducer=self.reducer_count_title)
        ]
    def mapper_get_title(self, _,line):
        (amount_requested, application_date, loan_title, risk_score, debt_to_income_ratio,state, employment_length) = line.split(",")
        yield loan_title, 1
            
        
    def reducer_count_title(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
   LoanTitleBreakdown.run()

