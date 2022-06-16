from abc import ABC, abstractmethod
import numpy as np
from typing import Tuple
class AbstractMarket(ABC):
    '''
    Abstract class from which market models should inherit

    defines common methods for all market models.
    '''

    @property
    @abstractmethod
    def prices(self):
        """
        A list of prices, beginning with the default price.
        """
        pass

    @property
    @abstractmethod
    def dividends(self):
        """
        A list of prices, beginning with the default price.
        """
        pass

    @abstractmethod
    def run_market(self) -> tuple([float, float]):
        """
        Runs the market for one day and returns the price.
        """
        price = 100
        dividend = 5.0 / 60
        return price, dividend

    @abstractmethod
    def get_simulation_price(self, seed: int, buy_sell: Tuple[int, int]):
        # does this need to be an abstract method or can it be encapsulated in daily_rate_of_return?
        pass

    @abstractmethod
    def daily_rate_of_return(self, seed: int, buy_sell: Tuple[int, int]):
        """
        Just the ROR of the price, not including the dividend.
        """
        pass

    @abstractmethod
    def close_market():
        pass

    def asset_price_stats(self):
        """
        Get statistics on the price of the asset for final reporting.
        """
        price_stats = {}

        price_stats['min_asset_price'] = min(self.prices)
        price_stats['max_asset_price'] = max(self.prices)

        price_stats['idx_min_asset_price'] = np.argmin(self.prices)
        price_stats['idx_max_asset_price'] = np.argmax(self.prices)

        price_stats['mean_asset_price'] = np.mean(self.prices)
        price_stats['std_asset_price'] = np.std(self.prices)

        return price_stats

    def dummy_run(self):
        """
        This acts as if the market 'ran' for one day, but uses the most recent rate of return
        to compute the next price without any stochasticity.
        """
        price = self.prices[-1] / self.prices[-2] * self.prices[-1]
        self.prices.append(price)

        price_to_dividend_ratio = 60 / 0.05
        dividend = price / price_to_dividend_ratio
        self.dividends.append(dividend)
        
        return price, dividend

    def ror_list(self):
        """
        Get a list of the rates of return, INCLUDING the dividend.
        Note the difference with daily_rate_of_return.
        This should be refactored for clarity.

        TODO: THIS WON'T WORK WITH SOME MARKETS WITH A DIFFERENT ROR CALCULATION?
        """
        return [((self.prices[i+1] + self.dividends[i + 1])/ self.prices[i]) - 1 for i in range(len(self.prices) - 1)]

class MockMarket(AbstractMarket):
    """
    A wrapper around the Market PNL model with methods for getting
    data from recent runs.

    Parameters
    ----------
    config_file
    config_local_file

    """
    simulation_price_scale = 1.0
    default_sim_price = 100

    # Empirical data -- redundant with FinanceModel!
    sp500_ror = 0.000628
    sp500_std = 0.011988

    # Storing the last market arguments used for easy access to most
    # recent data
    last_buy_sell = None
    last_seed = None

    seeds = []

    prices = None
    dividends = None

    def __init__(self):
        self.prices = [self.default_sim_price]
        self.dividends = [0]
        pass

    def run_market(self, seed=0, buy_sell=(0,0)):
        """
        Sample from a probability distribution
        """
        self.last_seed = seed
        self.last_buy_sell = buy_sell

        print("run_market, buy_sell: " + str(buy_sell))

        # target ror of the price distribution with no broker impact
        price_ror = self.prices[-1] * (1 + self.sp500_ror)
        # target variance of the price distribution with no broker impact
        price_std = self.prices[-1] * self.sp500_std

        # mean of underlying normal distribution
        exp_ror = np.log((price_ror ** 2) / np.sqrt(price_ror ** 2 + price_std ** 2))
        # standard deviation of underlying distribution
        exp_std = np.sqrt(np.log( 1 + price_std ** 2 / price_ror ** 2))

        # broken code reflecting price impact
        # mean = 0.000628 + np.log1p(np.float64(buy_sell[0])) - np.log1p(np.float64(buy_sell[1]))
        # std = 1 + np.log1p(np.float64(buy_sell[0] + buy_sell[1]))
        
        price = np.random.lognormal(exp_ror, exp_std)

        self.prices.append(price) ## TODO: Should this be when the new rate of return is computed?

        print('price: ' + str(price))

        # discounted future value, divided by days per quarter
        price_to_dividend_ratio = 60 / 0.05
        dividend = price / price_to_dividend_ratio
        self.dividends.append(dividend)

        return price, dividend

    def get_simulation_price(self, seed=0, buy_sell=(0, 0)):
        """
        Get the price from the simulation run.

        TODO: Refactor this -- the original PNL market was convoluted and this API can be streamlined.
        """

        return self.prices[-1]

    def daily_rate_of_return(self, seed=None, buy_sell=None):

        if seed is None:
            seed = self.last_seed

        if buy_sell is None:
            buy_sell = self.last_buy_sell

        last_sim_price = self.get_simulation_price(seed=seed, buy_sell=buy_sell)

        #if last_sim_price is None:
        #   last_sim_price = self.default_sim_price

        # ror = (last_sim_price * self.simulation_price_scale - 100) / 100
        ror = (self.prices[-1] - self.prices[-2])/self.prices[-2]

        return ror

    def close_market(self):
        return
