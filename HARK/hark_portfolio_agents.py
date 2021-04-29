import HARK.ConsumptionSaving.ConsPortfolioModel as cpm
import HARK.ConsumptionSaving.ConsIndShockModel as cism
from HARK.core import distribute_params
from HARK.distribution import Uniform
import math
import numpy as np
import pandas as pd
from statistics import mean


import sys

sys.path.append('.')
## TODO configuration file for this value!
sys.path.append('../PNL/py')

import util as UTIL
import pnl as pnl

import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)


### Initializing agents

def update_return(dict1, dict2):
    """
    Returns new dictionary,
    copying dict1 and updating the values of dict2
    """
    dict3 = dict1.copy()
    dict3.update(dict2)

    return dict3

import HARK.ConsumptionSaving.ConsPortfolioModel as cpm
import HARK.ConsumptionSaving.ConsIndShockModel as cism
from HARK.core import distribute_params
from HARK.distribution import Uniform

def initialize_agents(agent_classes, agent_parameters):
    """
    Initialize the agent objects according to standard
    parameterization agent_parameters and agent_classes definition.

    Parameters
    ----------

    agent_classes: list of dicts
        Parameters for each HARK AgentType
        # TODO: Rename, conflict with Python 'class' term

    agent_parameters: dict
        Parameters shared by all agents (unless overwritten).

    Returns
    -------
        agents: a list of HARK agents.
    """
    agents = [
        cpm.PortfolioConsumerType(
            **update_return(agent_parameters, ac)
        )
        for ac
        in agent_classes
    ]
    
    return agents

def distribute_beta(agents):
    """
    Distribue the discount rate among a set of agents according
    the distribution from Carroll et al., "Distribution of Wealth"
    paper.

    Parameters
    ----------

    agents: list of AgentType
        A list of AgentType

    Returns
    -------
        agents: A list of AgentType
    """
 
    # This is hacky. Should streamline this in HARK.
    agents_distributed = [
        distribute_params(
            agent,
            'DiscFac',
            5,
            Uniform(bot=0.936, top=0.978)
        ) 
        for agent in agents
    ]
    
    agents = [
        agent
        for agent_dist in agents_distributed
        for agent in agent_dist
    ]
    
    # should be unecessary but a hack to cover a HARK bug
    # https://github.com/econ-ark/HARK/issues/994
    for agent in agents:
        agent.assign_parameters(
            DiscFac = agent.DiscFac,
            AgentCount = agent.AgentCount
        )
    return agents

def create_distributed_agents(agent_classes, agent_parameters):
    """
    Creates agents of the given classes with stable parameters.
    Will overwrite the DiscFac with a distribution from CSTW_MPC.
    """
    
    agents = initialize_agents(agent_classes, agent_parameters)
    agents = distribute_beta(agents)

    return agents

def init_simulations(agents):
    """
    Sets up the agents with their state for the state of the simulation
    """
    for agent in agents:
        agent.track_vars += ['pLvl','mNrm','cNrm','Share','Risky']

        agent.assign_parameters(AdjustPrb = 1.0)
        agent.T_sim = 1000 # arbitrary!
        agent.solve()

        ### TODO: make and equivalent PF model and solve it
        ### to get the steady-state wealth
        ### This code block is not working.
        #pf_clone = cism.PerfForesightConsumerType(**agent.parameters)
        #pf_clone.assign_parameters(Rfree = pf_clone.parameters['RiskyAvg'])
        #pf_clone.solve()
        #
        #if not hasattr(pf_clone.solution[0],'mNrmStE'):
        #    # See https://github.com/econ-ark/HARK/issues/1005
        #    x = 0.7
        #    print(f"Agent has no steady state normalized market resources. Using {x} as stopgap.")
        #    pf_clone.solution[0].mNrmStE = x # A hack.

        agent.initialize_sim()

        # set normalize assets to steady state market resources.
        agent.state_now['mNrm'][:] = 1.0 #pf_clone.solution[0].mNrmStE
        agent.state_now['aNrm'] = agent.state_now['mNrm'] - agent.solution[0].cFuncAdj(agent.state_now['mNrm'])
        agent.state_now['aLvl'] = agent.state_now['aNrm'] * agent.state_now['pLvl']

    return agents



### Initializing financial values

### These are used for the agent's starting estimations
### of the risky asset

market_rate_of_return = 0.000628
market_standard_deviation = 0.011988



######
#   Math Utilities
######

def ror_quarterly(ror, n_q):
    """
    Convert a daily rate of return to a rate for (n_q) days.
    """
    return pow(1 + ror, n_q) - 1

def sig_quarterly(std, n_q):
    """
    Convert a daily standard deviation to a standard deviation for (n_q) days.

    This formula only holds for special cases (see paper by Andrew Lo), 
    but since we are generating the data we meet this special case.
    """
    return math.sqrt(n_q) * std

def lognormal_moments_to_normal(mu_x, std_x):
    """
    Given a mean and standard deviation of a lognormal distribution,
    return the corresponding mu and sigma for the distribution.
    (That is, the mean and standard deviation of the "underlying"
    normal distribution.)
    """
    mu = np.log(mu_x ** 2 / math.sqrt(mu_x ** 2 + std_x ** 2))

    sigma = math.sqrt(np.log(1 + std_x ** 2 / mu_x ** 2))

    return mu, sigma

def combine_lognormal_rates(ror1, std1, ror2, std2):
    """
    Given two mean rates of return and standard deviations
    of two lognormal processes (ror1, std1, ror2, std2),
    return a third ror/sigma pair corresponding to the
    moments of the multiplication of the two original processes.
    """
    mean1 = 1 + ror1
    mean2 = 1 + ror2

    mu1, sigma1 = lognormal_moments_to_normal(mean1, std1)
    mu2, sigma2 = lognormal_moments_to_normal(mean2, std2)

    mu3 = mu1 + mu2
    var3 = sigma1 **2 + sigma2 ** 2

    ror3 = math.exp(mu3 + var3 / 2) - 1
    sigma3 = math.sqrt((math.exp(var3) - 1) * math.exp(2 * mu3 + var3))

    return ror3, sigma3


######
#   PNL Interface methods
######

class MarketPNL():
    netlogo_ror = -0.00052125
    netlogo_std =  0.0068
    simulation_price_scale = 0.25
    default_sim_price = 400

    # Empirical data -- redundant with FinanceModel!
    sp500_ror = 0.000628
    sp500_std = 0.011988

    #
    last_buy_sell = None
    last_seed = None

    def __init__(
        self,
        config_file = "../PNL/macroliquidity.ini",
        config_local_file = "../PNL/macroliquidity_local.ini"
    ):
        self.config = UTIL.read_config(
            config_file = config_file,
            config_local_file = config_local_file
        )
    
    def run_market(self, seed = 0, buy_sell = 0):
        """
        Runs the NetLogo market simulation with a given
        configuration (config), a tuple with the quantities
        for the brokers to buy/sell (buy_sell), and
        optionally a random seed (seed)
        """
        self.last_seed = seed
        self.last_buy_sell = buy_sell

        pnl.run_NLsims(
            self.config,
            broker_buy_limit = buy_sell[0],
            broker_sell_limit = buy_sell[1],
            SEED = seed,
            use_cache = True
        )

    def get_transactions(self, seed = 0, buy_sell = (0,0)):
        """
        Given a random seed (seed)
        and a tuple of buy/sell (buy_sell), look up the transactions
        from the associated output file and return it as a pandas DataFrame.
        """
        logfile = pnl.transaction_file_name(
            self.config,
            seed,
            buy_sell[0],
            buy_sell[1]
        )

        # use run_market() first to create logs
        transactions = pd.read_csv(
            logfile,
            delimiter='\t'
        )
        return transactions

    def get_simulation_price(self, seed = 0, buy_sell = (0,0)):
        """
        Get the price from the simulation run.
        Returns None if the transaction file was empty for some reason.

        TODO: Better docstring
        """

        transactions = self.get_transactions(seed=seed, buy_sell = buy_sell)
        prices = transactions['TrdPrice']

        if len(prices.index) == 0:
            ## BUG FIX HACK
            print("ERROR in PNL script: zero transactions. Reporting no change")
            return None

        return prices[prices.index.values[-1]]


    def daily_rate_of_return(self, seed = None, buy_sell = None):
        ## TODO: Cleanup. Use "last" parameter in just one place.
        if seed is None:
            seed = self.last_seed

        if buy_sell is None:
            buy_sell = self.last_buy_sell

        last_sim_price = self.get_simulation_price(
            seed=seed, buy_sell = buy_sell
        )

        if last_sim_price is None:
            last_sim_price = self.default_sim_price

        ror = (last_sim_price * self.simulation_price_scale - 100) / 100

        # adjust to calibrated NetLogo to S&P500
        ror = self.sp500_std * (ror - self.netlogo_ror) / self.netlogo_std + self.sp500_ror

        return ror

