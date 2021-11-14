#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from mrjob.job import MRJob
from mrjob.step import MRStep


class AcceptedLoanPurpose(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_purpose,
                   reducer=self.reducer_count_purpose)
        ]
    def mapper_get_purpose(self, _,line):
        (loan_amnt, funded_amnt, term, int_rate, emp_title,emp_length, home_ownership, purpose,disbursement_method,
         loan_title ) = line.split(",")
        yield purpose, 1
            
        
    def reducer_count_purpose(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
   AcceptedLoanPurpose.run()

