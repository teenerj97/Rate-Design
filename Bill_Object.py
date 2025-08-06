import polars as pl
import scipy.stats as st
import geopandas as gpd

class Bill:
    def __init__(self, fuel, name, tariff, upgrade, ids, distribution, weights):
        self.fuel = fuel
        self.name = name
        self.tariff = tariff
        self.upgrade = upgrade
        self.ids = ids
        self.distribution = distribution
        weights = [float(w) for w in weights]
        self.weights = weights
        self.n = len(distribution)

        if self.n > 0:
            # Calculate Average
            self.average = sum(dist*weight for dist,weight in zip(distribution,weights))/sum(weights)

            # Calculate the margin of error
            den = sum(weights) - sum(wi*wi for wi in weights)/sum(weights)
            wv = sum(wi*(ci - self.average)**2 for ci,wi in zip(distribution, weights)) / max(den,1)
            n_eff = sum(weights)**2 / sum(wi*wi for wi in weights)
            se = (wv / n_eff)**0.5
            self.moe = st.norm.ppf(0.95) * se
        else:
            self.average = self.moe = 0

    def __str__(self):
        if "+" not in self.fuel:
            return f"{'Upgrade measure '+str(self.upgrade) if self.upgrade else 'Baseline Profile'} for {self.name} electric tariff: ${self.average:,.2f} ± {self.moe:,.2f}"
        else:
            return f"{self.name} with {'upgrade measure '+str(self.upgrade) if self.upgrade else 'baseline profile'}: ${self.average:,.2f} ± {self.moe:,.2f}"
    
    def __repr__(self):
        return f"Bill(fuel={self.fuel}, name={self.name}, upgrade={self.upgrade}, average={self.average}, moe={self.moe})"
    
    def __len__(self):
        return len(self.distribution)

    def __add__(self, other):
        if isinstance(other, Bill) and ((self.fuel == "electric" and other.fuel == "gas") or (self.fuel == "gas" and other.fuel == "electric")):
            weights = [min(w1,w2) for w1,w2 in zip(self.weights,other.weights)]
            distribution = [s+o for s,o in zip(self.distribution,other.distribution)]
            return Bill(f"{self.fuel}+{other.fuel}", self.name if self.fuel=="electric" else other.name, self.tariff if self.fuel=="electric" else other.tariff, self.upgrade, self.ids, distribution, weights)
        raise ValueError("Type Error, or is not an electric and gas bill addition")
    
    def __sub__(self, other):
        if isinstance(other, Bill) and self.fuel==other.fuel:
            distribution = [s-o for s,o in zip(self.distribution,other.distribution)]
            return Bill(self.fuel, self.name, self.tariff, self.upgrade, self.ids, distribution, self.weights)
        raise ValueError("Type Error or comparing bills from two different fuels (doesn't make sense!)")
    
    def combine_bills(self, other):
        if isinstance(other, Bill) and self.fuel==other.fuel:
            distribution = self.distribution + other.distribution
            ids = self.ids + other.ids
            weights = self.weights + other.weights
            ids, distribution, weights = zip(*sorted(zip(ids, distribution, weights)))
            return Bill(self.fuel, f"Combined bills of {self.name} and {other.name}", (self.tariff, other.tariff), self.upgrade, list(ids), list(distribution), list(weights))
        raise ValueError("Type error or combining bills of different fuel types")

    def select(self, id_select):
        if isinstance(id_select,list):
            ids = [id for id in self.ids if id in id_select]
            distribution = [dist for id,dist in zip(self.ids,self.distribution) if id in id_select]
            weights = [weight for id,weight in zip(self.ids,self.weights) if id in id_select]
            ids, distribution, weights = zip(*sorted(zip(ids, distribution, weights))) # just in case
            return Bill(self.fuel, self.name, self.tariff, self.upgrade, list(ids), list(distribution), list(weights))
        raise ValueError("IDs not given as a list")
    
    def build_df(self, buildings):
        if isinstance(buildings, pl.DataFrame):
            df = pl.DataFrame({"bldg_id":self.ids,"distribution":self.distribution})
            df = buildings.filter(pl.col("bldg_id").is_in(df["bldg_id"])).join(df,on="bldg_id")
            return df
        raise ValueError("Buildings df passed is not a polars df")
    
    def build_geo_object(self, buildings):
        buildings = self.build_df(buildings).to_pandas()
        buildings = gpd.GeoDataFrame(
            buildings,
            geometry=gpd.points_from_xy(buildings["in.weather_file_longitude"], buildings["in.weather_file_latitude"]),
            crs="EPSG:4326"
        )
        return buildings
    
class BillList:
    def __init__(self, bills=None):
        self.bills = bills if bills is not None else []

    def append(self, bill):
        if isinstance(bill, Bill):
            self.bills.append(bill)
        else:
            raise ValueError(f"Can not append non bill object {bill}")

    def __getitem__(self, key):
        """"
        Provide an upgrade and tariff in that order as a tuple/list
        """
        if isinstance(key, tuple) and len(self.bills)>0:
            return [bill for bill in self.bills if bill.upgrade ==key[0] and (bill.tariff == key[1] if bill.fuel!="gas" else True)][0]
        elif isinstance(key, int) and len(self.bills)>0:
            return self.bills[key]
        elif len(self.bills) == 0:
            return Bill(0,0,0,0,[],[],[])
        raise ValueError("Invalid key")
    
    def __iter__(self):
        return iter(self.bills)
    