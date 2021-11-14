#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from mrjob.job import MRJob
from mrjob.step import MRStep


class PaymentMethodBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_method,
                   reducer=self.reducer_count_method)
        ]
    def mapper_get_method(self, _,line):
        (loan_amnt, funded_amnt, term, int_rate, emp_title,emp_length, home_ownership, purpose,disbursement_method,
         loan_title ) = line.split(",")
        yield disbursement_method, 1
            
        
    def reducer_count_method(self, key, values):
        yield key, sum(values)

if __name__ == '__main__':
   PaymentMethodBreakdownn.run()

