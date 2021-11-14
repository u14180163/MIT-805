#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from mrjob.job import MRJob
from mrjob.step import MRStep


class EmploymentLengthBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_length,
                   reducer=self.reducer_count_length)
        ]
    def mapper_get_length(self, _,line):
        (amount_requested, application_date, loan_title, risk_score, debt_to_income_ratio,state, employment_length) = line.split(",")
        yield employment_length, 1
            
        
    def reducer_count_length(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
   EmploymentLengthBreakdown.run()

