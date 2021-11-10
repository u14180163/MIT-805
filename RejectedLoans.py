#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from mrjob.job import MRJob
from mrjob,step import MRStep

"The following mapper and reducer counts and summarises the job titles of clients that were rejected for loans" 
class LoanTitleBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_loan_title,
                 reducer=self.reducer_count_loan_title)
        ]
    def mapper_get_loan_title(self, _,line):
        (loan_title) = line.split(",")
        yield loan_title, 1
        
        
    def reducer_count_loan_title(self, key, values):
        yield key, sum(values)
        
if __name == '__main__':
    LoanTitleBreakdown.run()
    
    
"The following mapper and reducer counts and summarises the employment length of clients that were rejected for loans" 
class EmploymentLengthBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_emp_length,
                 reducer=self.reducer_count_emp_length)
        ]
    def mapper_get_emp_length(self, _,line):
        (loan_title) = line.split(",")
        yield loan_title, 1
        
        
    def reducer_count_emp_length(self, key, values):
        yield key, sum(values)
        
if __name == '__main__':
    EmploymentLengthBreakdown.run()
    

    


    
    
    

