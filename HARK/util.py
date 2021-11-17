from itertools import chain

class AgentList:
	def __init__(self, agents, dist_params):
		self.agent_cats = []
		self.idx = -1

	    """
	    Distribue the discount rate among a set of agents according
	    the distribution from Carroll et al., "Distribution of Wealth"
	    paper.

	    Parameters
	    ----------

	    agents: list of AgentType
	        A list of AgentType

	    dist_params:

	    Returns
	    -------
	        agents: A list of AgentType
	    """
	 
	    # This is hacky. Should streamline this in HARK.

	    for param in dist_params:
	        agents_distributed = [
	            distribute_params(
	                agent,
	                param,
	                dist_params[param]['n'],
	                Uniform(
	                    bot=dist_params[param]['bot'],
	                    top=dist_params[param]['top']
	                )
	            )
	            for agent in agents
	        ]

	        for agent_dist in agents_distributed:
	            for agent in agent_dist:
	                agent.seed = random.randint(0,100000000)
	                agent.reset_rng()
	                agent.IncShkDstn[0].seed = random.randint(0,100000000)
	                agent.IncShkDstn[0].reset()

	    self.agent_cats = agents_distributed

	    self.flattened = list(chain(*self.agent_cats))
    #     agents = [
    #         agent
    #         for agent_dist in agents_distributed
    #         for agent in agent_dist
    #     ]

    # return agents

    def __iter__(self):
    	return self

    def __next__(self):
    	self.idx += 1

    	if self.idx >= len(self.flattened):
    		raise StopIteration

    	return self.flattened(self.idx)


    	

