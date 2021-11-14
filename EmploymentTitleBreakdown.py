#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from mrjob.job import MRJob
from mrjob.step import MRStep


class EmploymentTitleBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_title,
                   reducer=self.reducer_count_title)
        ]
    def mapper_get_title(self, _,line):
        (loan_amnt, funded_amnt, term, int_rate, emp_title,emp_length, home_ownership, purpose,disbursement_method,
         loan_title ) = line.split(",")
        yield emp_title, 1
            
        
    def reducer_count_title(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
   EmploymentTitleBreakdown.run()

