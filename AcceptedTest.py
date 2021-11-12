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

