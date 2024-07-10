use crate::stock_c_ffi;
use chrono::{Datelike, NaiveDate};
use ndarray::{arr1, Axis};
use ndarray_stats::{interpolate::Linear, QuantileExt};
use noisy_float::types::n64;
use std::collections::{HashMap, HashSet};
use std::ffi::CStr;
use std::fmt::Debug;
use std::fs::File;
use std::rc::Rc;

#[derive(Debug)]
pub struct Price {
    pub date: NaiveDate,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub close_adj: f64,
    pub volume: u64,
}

impl From<stock_c_ffi::Price> for Price {
    fn from(p: stock_c_ffi::Price) -> Self {
        let c_str = unsafe {
            assert!(!p.date.is_null());

            CStr::from_ptr(p.date)
        };
        let date = c_str.to_str().unwrap();

        let date = NaiveDate::parse_from_str(date, "%Y-%m-%d")
            .unwrap_or_else(|_| panic!("price date {} 轉換失敗，格式為 %Y-%m-%d", date));

        Price {
            date,
            open: p.open,
            high: p.high,
            low: p.low,
            close: p.close,
            close_adj: p.close_adj,
            volume: p.volume,
        }
    }
}

impl From<&stock_c_ffi::Price> for Price {
    fn from(p: &stock_c_ffi::Price) -> Self {
        let c_str = unsafe {
            assert!(!p.date.is_null());

            CStr::from_ptr(p.date)
        };
        let date = c_str.to_str().unwrap();

        let date = NaiveDate::parse_from_str(date, "%Y-%m-%d")
            .unwrap_or_else(|_| panic!("price date {} 轉換失敗，格式為 %Y-%m-%d", date));

        Price {
            date,
            open: p.open,
            high: p.high,
            low: p.low,
            close: p.close,
            close_adj: p.close_adj,
            volume: p.volume,
        }
    }
}

#[allow(dead_code)]
#[derive(Debug)]
struct Return {
    start: NaiveDate,
    end: NaiveDate,
    value: f64,
}

#[allow(dead_code)]
#[derive(Debug)]
pub struct Stock {
    symbol: String,
    data: Vec<Price>,
    years: Option<HashSet<i32>>,
    all_return: Option<Vec<Rc<Return>>>,
    years_return: Option<HashMap<i32, Vec<Rc<Return>>>>,
}

#[derive(Debug)]
pub struct Stat {
    pub count: usize,
    pub mean: f64,
    pub std: f64,
    pub min: f64,
    pub q1: f64,
    pub q2: f64,
    pub q3: f64,
    pub max: f64,
}

impl Stock {
    pub fn new(data: &[Price]) -> Result<Stock, String> {
        let mut stock = Stock {
            symbol: String::from("symbol"),
            data: Vec::new(),
            years: None,
            all_return: None,
            years_return: None,
        };

        for price in data.iter() {
            stock.data.push(Price { ..*price });
        }
        stock.cal_return();
        Ok(stock)
    }
    pub fn new_by_csv(symbol: &str, file_path: &str) -> Result<Stock, String> {
        let file = File::open(file_path).expect(&format!("開啟 {} 失敗", file_path)[..]);
        let mut reader = csv::Reader::from_reader(file);

        let mut stock = Stock {
            symbol: String::from(symbol),
            data: Vec::new(),
            years: None,
            all_return: None,
            years_return: None,
        };

        for record in reader.records() {
            let mut record = record.expect("reader.records() Fail");
            record.trim();

            let date = NaiveDate::parse_from_str(&record[0], "%Y-%m-%d").unwrap_or_else(|_| panic!(
                "price date {} 轉換失敗，格式為 %Y-%m-%d",
                &record[0]
            ));
            let open: f64 = record[1]
                .parse()
                .unwrap_or_else(|_| panic!("price open {} 轉換失敗", &record[1]));
            let high: f64 = record[2]
                .parse()
                .unwrap_or_else(|_| panic!("price high {} 轉換失敗", &record[2]));
            let low: f64 = record[3]
                .parse()
                .unwrap_or_else(|_| panic!("price low {} 轉換失敗", &record[3]));
            let close: f64 = record[4]
                .parse()
                .unwrap_or_else(|_| panic!("price close {} 轉換失敗", &record[4]));
            let close_adj: f64 = record[5]
                .parse()
                .unwrap_or_else(|_| panic!("price close_adj {} 轉換失敗", &record[5]));
            let volume: u64 = record[6]
                .parse()
                .unwrap_or_else(|_| panic!("price volume {} 轉換失敗", &record[6]));

            let price = Price {
                date,
                open,
                high,
                low,
                close,
                close_adj,
                volume,
            };
            stock.data.push(price);
        }
        stock.cal_return();
        Ok(stock)
    }

    fn cal_return(&mut self) {
        let d = NaiveDate::from_ymd_opt(2015, 3, 14).unwrap();
        assert_eq!(d.year(), 2015);

        if self.all_return.is_some() {
            return;
        }

        let mut result: Vec<Rc<Return>> = Vec::new();
        let mut years_result: HashMap<i32, Vec<Rc<Return>>> = HashMap::new();
        for (i, start) in self.data.iter().enumerate() {
            let start_year = start.date.year();
            for end in self.data[i + 1..].iter() {
                let r = Rc::new(Return {
                    start: start.date,
                    end: end.date,
                    value: (end.close_adj - start.close_adj) / start.close_adj,
                });
                if start_year == end.date.year() {
                    years_result
                        .entry(start_year)
                        .or_default()
                        .push(Rc::clone(&r));
                }
                result.push(Rc::clone(&r));
            }
        }
        self.years_return = Some(years_result);
        self.all_return = Some(result);
    }

    pub fn n_years(&self) -> usize {
        self.years_return
            .as_ref()
            .expect("尚未提供資料分析，無法提供年數")
            .keys()
            .len()
    }

    fn cal_years(&mut self) {
        if self.years.is_some() {
            return;
        }

        let mut years = HashSet::new();
        for y in self.data.iter().map(|x| x.date.year()) {
            years.insert(y);
        }
        self.years = Some(years);
    }

    fn cal_statistic(data: &[f64]) -> Stat {
        let mut data = arr1(data);
        let mean = data.mean().unwrap();
        let std = data.std(1.);
        let min = *data.min().unwrap();

        let q1 = data
            .quantile_axis_skipnan_mut(Axis(0), n64(0.25), &Linear)
            .unwrap();
        let q1 = q1[()];
        let q2 = data
            .quantile_axis_skipnan_mut(Axis(0), n64(0.5), &Linear)
            .unwrap();
        let q2 = q2[()];
        let q3 = data
            .quantile_axis_skipnan_mut(Axis(0), n64(0.75), &Linear)
            .unwrap();
        let q3 = q3[()];
        let max = *data.max().unwrap();
        Stat {
            count: data.len(),
            mean,
            std,
            min,
            q1,
            q2,
            q3,
            max,
        }
    }

    pub fn stat_active_all(&mut self) -> Stat {
        self.cal_return();

        let data: Vec<f64> = self
            .all_return
            .as_ref()
            .unwrap()
            .iter()
            .map(|x| x.value)
            .collect();

        Stock::cal_statistic(&data[..])
    }

    pub fn stat_hold_all(&mut self) -> Stat {
        self.cal_return();

        let end = self.data[self.data.len() - 1].date;

        let data: Vec<f64> = self
            .all_return
            .as_ref()
            .unwrap()
            .iter()
            .filter(|x| x.end == end)
            .map(|x| x.value)
            .collect();

        Stock::cal_statistic(&data[..])
    }

    pub fn stat_active_year(&mut self) -> Vec<(i32, Stat)> {
        self.cal_return();

        let mut result: Vec<(i32, Stat)> = Vec::new();
        for (year, data) in self.years_return.as_ref().unwrap().iter() {
            let data: Vec<f64> = data.iter().map(|x| x.value).collect();

            let stat = Stock::cal_statistic(&data[..]);

            result.push((*year, stat));
        }

        result
    }

    pub fn stat_hold_year(&mut self) -> Vec<(i32, Stat)> {
        self.cal_return();

        let mut result: Vec<(i32, Stat)> = Vec::new();
        for (year, data) in self.years_return.as_ref().unwrap().iter() {
            let end_date = data[data.len() - 1].end;
            let data: Vec<f64> = data
                .iter()
                .filter(|x| x.end == end_date)
                .map(|x| x.value)
                .collect();

            let stat = Stock::cal_statistic(&data[..]);

            result.push((*year, stat));
        }

        result
    }

    pub fn cal_years_return(&mut self) -> Vec<(i32, f64)> {
        self.cal_years();

        let mut result: Vec<(i32, f64)> = Vec::new();

        for year in self.years.as_ref().unwrap() {
            let data: Vec<_> = self
                .data
                .iter()
                .filter(|x| x.date.format("%Y").to_string() == year.to_string())
                .collect();

            let start = data[0];
            let end = data[data.len() - 1];
            let r = (end.close_adj - start.close_adj) / start.close_adj;

            result.push((*year, r));
        }

        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_csv() {
        let stock = Stock::new_by_csv("VTI", "tests//data.csv");
        match stock {
            Ok(stock) => {
                assert_eq!(stock.symbol, "VTI");
                assert!(!stock.data.is_empty());
                println!("{:?}", stock.data[0]);
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_cal_return() {
        let stock = Stock::new_by_csv("VTI", "tests//data.csv");
        match stock {
            Ok(mut stock) => {
                stock.cal_return();
                let result = &stock.all_return.unwrap();
                assert_eq!(result.len(), stock.data.len() * (stock.data.len() - 1) / 2);
                println!("{:?}", result[0]);
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_cal_years() {
        let stock = Stock::new_by_csv("VTI", "tests//data.csv");
        match stock {
            Ok(mut stock) => {
                stock.cal_years();
                let ys = stock.years.unwrap();
                assert!(ys.contains(&2010));
                assert!(ys.contains(&2011));
                assert!(ys.contains(&2012));
                assert!(ys.contains(&2013));
                assert!(ys.contains(&2014));
                assert!(ys.contains(&2015));
                assert!(ys.contains(&2016));
                assert!(ys.contains(&2017));
                assert!(ys.contains(&2018));
                assert!(ys.contains(&2019));
                assert!(ys.contains(&2020));
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_cal_statistic() {
        let stock = Stock::new_by_csv("VTI", "tests//data.csv");
        match stock {
            Ok(mut stock) => {
                stock.cal_return();
                let data: Vec<f64> = stock.all_return.unwrap().iter().map(|x| x.value).collect();
                let stat = Stock::cal_statistic(&data[..]);
                println!("{:?}", stat);
                assert_eq!(stat.count, 4048435);
                assert_eq!(stat.mean, 0.7006803471800115);
                assert_eq!(stat.std, 0.6504696988790459);
                assert_eq!(stat.min, -0.35000285888869426);
                assert_eq!(stat.q1, 0.20295587686363614);
                assert_eq!(stat.q2, 0.5193659465054783);
                assert_eq!(stat.q3, 1.0194780191566912);
                assert_eq!(stat.max, 4.125857367549767);
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_stat_active_all() {
        let stock = Stock::new_by_csv("VTI", "tests//data.csv");
        match stock {
            Ok(mut stock) => {
                let stat = stock.stat_active_all();
                println!("{:?}", stat);
                assert_eq!(stat.count, 4048435);
                assert_eq!(stat.mean, 0.7006803471800115);
                assert_eq!(stat.std, 0.6504696988790459);
                assert_eq!(stat.min, -0.35000285888869426);
                assert_eq!(stat.q1, 0.20295587686363614);
                assert_eq!(stat.q2, 0.5193659465054783);
                assert_eq!(stat.q3, 1.0194780191566912);
                assert_eq!(stat.max, 4.125857367549767);
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_stat_hold_all() {
        let stock = Stock::new_by_csv("VTI", "tests//data.csv");
        match stock {
            Ok(mut stock) => {
                let stat = stock.stat_hold_all();
                println!("{:?}", stat);
                assert_eq!(stat.count, 2845);
                assert_eq!(stat.mean, 1.5543183705660275);
                assert_eq!(stat.std, 1.0578261490595642);
                assert_eq!(stat.min, 0.00027630670043749607);
                assert_eq!(stat.q1, 0.6460234865825328);
                assert_eq!(stat.q2, 1.2775028310195866);
                assert_eq!(stat.q3, 2.488169843311141);
                assert_eq!(stat.max, 4.125857367549767);
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_stat_active_year() {
        let mut answer: HashMap<i32, Stat> = HashMap::new();
        answer.insert(
            2010,
            Stat {
                count: 31626,
                mean: 0.037017844,
                std: 0.073642899,
                min: -0.162290081,
                q1: -0.014200332,
                q2: 0.033401081,
                q3: 0.087488856,
                max: 0.262035251,
            },
        );
        answer.insert(
            2011,
            Stat {
                count: 31626,
                mean: -0.030776179,
                std: 0.062213648,
                min: -0.203041106,
                q1: -0.078118511,
                q2: -0.027276818,
                q3: 0.015843744,
                max: 0.181687951,
            },
        );
        answer.insert(
            2012,
            Stat {
                count: 31125,
                mean: 0.032247184,
                std: 0.044807779,
                min: -0.100589879,
                q1: 0.001307694,
                q2: 0.031167747,
                q3: 0.063003249,
                max: 0.168663681,
            },
        );
        answer.insert(
            2013,
            Stat {
                count: 31626,
                mean: 0.082614458,
                std: 0.064634988,
                min: -0.05694196,
                q1: 0.032734853,
                q2: 0.072361398,
                q3: 0.123086933,
                max: 0.303718746,
            },
        );
        answer.insert(
            2014,
            Stat {
                count: 31626,
                mean: 0.045463069,
                std: 0.042984553,
                min: -0.075662874,
                q1: 0.013841044,
                q2: 0.042985845,
                q3: 0.072947714,
                max: 0.208122268,
            },
        );
        answer.insert(
            2015,
            Stat {
                count: 31626,
                mean: -0.004289436,
                std: 0.037263305,
                min: -0.120610684,
                q1: -0.025267753,
                q2: -0.002141399,
                q3: 0.018892788,
                max: 0.12171939,
            },
        );
        answer.insert(
            2016,
            Stat {
                count: 31626,
                mean: 0.066271663,
                std: 0.060238885,
                min: -0.101097457,
                q1: 0.020994931,
                q2: 0.0561365,
                q3: 0.101755591,
                max: 0.288358957,
            },
        );
        answer.insert(
            2017,
            Stat {
                count: 31375,
                mean: 0.05728463,
                std: 0.044796072,
                min: -0.027144263,
                q1: 0.022051136,
                q2: 0.048613165,
                q3: 0.086673267,
                max: 0.207987413,
            },
        );
        answer.insert(
            2018,
            Stat {
                count: 31375,
                mean: 0.006675123,
                std: 0.054403281,
                min: -0.200464711,
                q1: -0.026189333,
                q2: 0.010968156,
                q3: 0.043167915,
                max: 0.152243152,
            },
        );
        answer.insert(
            2019,
            Stat {
                count: 31626,
                mean: 0.059549264,
                std: 0.056488337,
                min: -0.06784831,
                q1: 0.018935897,
                q2: 0.049833745,
                q3: 0.090752371,
                max: 0.341621608,
            },
        );
        answer.insert(
            2020,
            Stat {
                count: 31878,
                mean: 0.104785725,
                std: 0.152938804,
                min: -0.350002885,
                q1: 0.015953895,
                q2: 0.086201042,
                q3: 0.181886956,
                max: 0.770215094,
            },
        );
        answer.insert(
            2021,
            Stat {
                count: 2926,
                mean: 0.031905695,
                std: 0.032138786,
                min: -0.049640954,
                q1: 0.008086597,
                q2: 0.030221072,
                q3: 0.052533891,
                max: 0.135841087,
            },
        );

        let stock = Stock::new_by_csv("VTI", "tests//data.csv");
        match stock {
            Ok(mut stock) => {
                let stat = stock.stat_active_year();
                println!("{:?}", stat);
                for (y, stat) in stat.iter() {
                    assert_eq!(stat.count, answer.get(y).unwrap().count);
                    assert!((stat.mean - answer.get(y).unwrap().mean).abs() < 0.000001);
                    assert!((stat.std - answer.get(y).unwrap().std).abs() < 0.000001);
                    assert!((stat.min - answer.get(y).unwrap().min).abs() < 0.000001);
                    assert!((stat.q1 - answer.get(y).unwrap().q1).abs() < 0.000001);
                    assert!((stat.q2 - answer.get(y).unwrap().q2).abs() < 0.000001);
                    assert!((stat.q3 - answer.get(y).unwrap().q3).abs() < 0.000001);
                    assert!((stat.max - answer.get(y).unwrap().max).abs() < 0.000001);
                }
            }
            Err(e) => panic!("{}", e),
        }
    }
    #[test]
    fn test_stat_hold_year() {
        let mut answer = HashMap::new();
        answer.insert(
            2010,
            Stat {
                count: 251,
                mean: 0.130642784,
                std: 0.061892951,
                min: -0.001691389,
                q1: 0.080334488,
                q2: 0.139160618,
                q3: 0.177483022,
                max: 0.259900659,
            },
        );
        answer.insert(
            2011,
            Stat {
                count: 251,
                mean: -0.001943153,
                std: 0.052350032,
                min: -0.076582327,
                q1: -0.042943055,
                q2: -0.012816191,
                q3: 0.034569491,
                max: 0.158676669,
            },
        );
        answer.insert(
            2012,
            Stat {
                count: 249,
                mean: 0.051032367,
                std: 0.039721599,
                min: -0.017456554,
                q1: 0.020771688,
                q2: 0.046580199,
                q3: 0.077694274,
                max: 0.148262843,
            },
        );
        answer.insert(
            2013,
            Stat {
                count: 251,
                mean: 0.145962012,
                std: 0.078593858,
                min: 0.003347189,
                q1: 0.092703972,
                q2: 0.139771566,
                q3: 0.210094638,
                max: 0.303718746,
            },
        );
        answer.insert(
            2014,
            Stat {
                count: 251,
                mean: 0.073435803,
                std: 0.045286186,
                min: -0.014777993,
                q1: 0.039326862,
                q2: 0.068251185,
                q3: 0.108939677,
                max: 0.190268636,
            },
        );
        answer.insert(
            2015,
            Stat {
                count: 251,
                mean: -0.008161181,
                std: 0.027093291,
                min: -0.043709099,
                q1: -0.028239378,
                q2: -0.018633993,
                q3: 0.005685133,
                max: 0.087448843,
            },
        );
        answer.insert(
            2016,
            Stat {
                count: 251,
                mean: 0.092854763,
                std: 0.06577007,
                min: -0.013262724,
                q1: 0.045459649,
                q2: 0.084553361,
                q3: 0.12383946,
                max: 0.271271795,
            },
        );
        answer.insert(
            2017,
            Stat {
                count: 250,
                mean: 0.1051275,
                std: 0.053135083,
                min: -0.004166133,
                q1: 0.065743785,
                q2: 0.110070709,
                q3: 0.146658134,
                max: 0.202954784,
            },
        );
        answer.insert(
            2018,
            Stat {
                count: 250,
                mean: -0.084226802,
                std: 0.035768881,
                min: -0.147496283,
                q1: -0.107993884,
                q2: -0.082825117,
                q3: -0.061865896,
                max: 0.066249013,
            },
        );
        answer.insert(
            2019,
            Stat {
                count: 251,
                mean: 0.116692708,
                std: 0.061193914,
                min: -0.003714367,
                q1: 0.079295944,
                q2: 0.110934347,
                q3: 0.150432758,
                max: 0.336638331,
            },
        );
        answer.insert(
            2020,
            Stat {
                count: 252,
                mean: 0.220279173,
                std: 0.148704605,
                min: 0.003092225,
                q1: 0.129739717,
                q2: 0.189051181,
                q3: 0.283518463,
                max: 0.770215094,
            },
        );
        answer.insert(
            2021,
            Stat {
                count: 76,
                mean: 0.065028885,
                std: 0.032195099,
                min: 0.000276366,
                q1: 0.05067601,
                q2: 0.064860862,
                q3: 0.088023754,
                max: 0.135841087,
            },
        );

        let stock = Stock::new_by_csv("VTI", "tests//data.csv");
        match stock {
            Ok(mut stock) => {
                let stat = stock.stat_hold_year();
                println!("{:?}", stat);
                for (y, stat) in stat.iter() {
                    assert_eq!(stat.count, answer.get(y).unwrap().count);
                    assert!((stat.mean - answer.get(y).unwrap().mean).abs() < 0.000001);
                    assert!((stat.std - answer.get(y).unwrap().std).abs() < 0.000001);
                    assert!((stat.min - answer.get(y).unwrap().min).abs() < 0.000001);
                    assert!((stat.q1 - answer.get(y).unwrap().q1).abs() < 0.000001);
                    assert!((stat.q2 - answer.get(y).unwrap().q2).abs() < 0.000001);
                    assert!((stat.q3 - answer.get(y).unwrap().q3).abs() < 0.000001);
                    assert!((stat.max - answer.get(y).unwrap().max).abs() < 0.000001);
                }
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_cal_years_return() {
        let mut answer = HashMap::new();
        answer.insert(2010, 0.15502202036080215);
        answer.insert(2011, -0.0006268116543881683);
        answer.insert(2012, 0.14826283829311068);
        answer.insert(2013, 0.3014632247183435);
        answer.insert(2014, 0.13543720640626467);
        answer.insert(2015, 0.004308299308666056);
        answer.insert(2016, 0.14530773777444983);
        answer.insert(2017, 0.20295479596526972);
        answer.insert(2018, -0.05899838826498521);
        answer.insert(2019, 0.30566311510323485);
        answer.insert(2020, 0.20078052090971935);
        answer.insert(2021, 0.13584106989211156);
        let stock = Stock::new_by_csv("VTI", "tests//data.csv");
        match stock {
            Ok(mut stock) => {
                let rs = stock.cal_years_return();
                println!("{:?}", rs);
                for (y, r) in rs.iter() {
                    assert_eq!(r, answer.get(y).unwrap());
                }
            }
            Err(e) => panic!("{}", e),
        }
    }

    #[test]
    fn test_n_years() {
        let stock = Stock::new_by_csv("VTI", "tests//data.csv");
        match stock {
            Ok(stock) => {
                assert_eq!(stock.n_years(), 12);
            }
            Err(e) => panic!("{}", e),
        }
    }
}
