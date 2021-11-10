#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from mrjob.job import MRJob
from mrjob,step import MRStep

"The following mapper and reducer counts and summarises the job titles of clinets that were accepted for loans" 
class EmplyomentTitleBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_titles,
                 reducer=self.reducer_count_titles)
        ]
    def mapper_get_titles(self, _,line):
        ("id", emp_title, purpose , addr_state) = line.split(",")
        yield emp_title, 1
        
        
    def reducer_count_titles(self, key, values):
        yield key, sum(values)
if __name == '__main__':
    EmplyomentTitleBreakdown.run()
    

"The following mapper and reducer counts and summarises the purposes for taking out a loan that clinets provided" 
from mrjob.job import MRJob
from mrjob,step import MRStep

class AcceptedLoanPurpose(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_purpose,
                 reducer=self.reducer_count_purpose)
        ]
    def mapper_get_purpose(self, _,line):
        (emp_title, purpose) = line.split(",")
        yield purpose, 1
        
        
    def reducer_count_purpose(self, key, values):
        yield key, sum(values)
if __name == '__main__':
    AcceptedLoanPurpose.run()
    

"The following mapper and reducer counts and summarises the home ownership of clients that recived loans"      
class HomeOwnershipBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_ownership,
                 reducer=self.reducer_count_ownership)
        ]
    def mapper_get_ownership(self, _,line):
        (emp_title, purpose, home_ownership,annual_inc) = line.split(",")
        yield home_ownership, 1
        
        
    def reducer_count_ownership(self, key, values):
        yield key, sum(values)
if __name == '__main__':
    AcceptedLoanPurpose.run()
    
    
    
"The following mapper and reducer counts and summarises the disbursement methods that customer requested"  
class PaymentMethodBreakdown(MRJob):
    def steps(self):
        return [
            MRStep(mapper=self.mapper_get_disbursement,
                 reducer=self.reducer_count_disbursement)
        ]
    def mapper_get_disbursement(self, _,line):
        (disbursement) = line.split(",")
        yield disbursement, 1
        
        
    def reducer_count_disbursement(self, key, values):
        yield key, sum(values)
if __name == '__main__':
    PaymentMethodBreakdow.run()
    
    
    
    

    
    
    

