use chrono::{Datelike, NaiveDate};
use pyo3::prelude::*;
use statrs::statistics;
use statrs::statistics::Distribution;
use statrs::statistics::Max;
use statrs::statistics::Min;
use statrs::statistics::OrderStatistics;
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::fs::File;

#[pyclass]
#[derive(Debug, Clone, Copy)]
pub struct Price {
    #[pyo3(get, set)]
    pub date: NaiveDate,
    #[pyo3(get, set)]
    pub open: f64,
    #[pyo3(get, set)]
    pub high: f64,
    #[pyo3(get, set)]
    pub low: f64,
    #[pyo3(get, set)]
    pub close: f64,
    #[pyo3(get, set)]
    pub close_adj: f64,
    #[pyo3(get, set)]
    pub volume: u64,
}

#[pymethods]
impl Price {
    #[new]
    pub fn new(
        date: NaiveDate,
        open: f64,
        high: f64,
        low: f64,
        close: f64,
        close_adj: f64,
        volume: u64,
    ) -> Self {
        Self {
            date,
            open,
            high,
            low,
            close,
            close_adj,
            volume,
        }
    }
}

#[pyclass]
#[derive(Debug, Clone, Copy)]
struct Return {
    #[pyo3(get, set)]
    start: NaiveDate,
    #[pyo3(get, set)]
    end: NaiveDate,
    #[pyo3(get, set)]
    value: f64,
}

#[pyclass]
#[derive(Debug)]
pub struct Stock {
    pub symbol: String,
    data: Vec<Price>,
    years: Option<HashSet<i32>>,
    all_return: Option<Vec<Return>>,
    years_return: Option<HashMap<i32, Vec<Return>>>,
}

#[pyclass]
#[derive(Debug, Clone, Copy)]
pub struct Stat {
    #[pyo3(get)]
    pub count: usize,
    #[pyo3(get)]
    pub mean: f64,
    #[pyo3(get)]
    pub std: f64,
    #[pyo3(get)]
    pub min: f64,
    #[pyo3(get)]
    pub q1: f64,
    #[pyo3(get)]
    pub q2: f64,
    #[pyo3(get)]
    pub q3: f64,
    #[pyo3(get)]
    pub max: f64,
}

#[pymethods]
impl Stock {
    #[new]
    pub fn new(data: Vec<Price>) -> Stock {
        let mut stock = Stock {
            symbol: String::from("symbol"),
            data: Vec::new(),
            years: None,
            all_return: None,
            years_return: None,
        };

        for price in data {
            stock.data.push(Price { ..price });
        }
        stock.cal_return();
        stock
    }

    pub fn n_years(&self) -> usize {
        self.years_return
            .as_ref()
            .expect("尚未提供資料分析，無法提供年數")
            .keys()
            .len()
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

        Stock::cal_statistic(data)
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

        Stock::cal_statistic(data)
    }

    pub fn stat_active_year(&mut self) -> Vec<(i32, Stat)> {
        self.cal_return();

        let mut result: Vec<(i32, Stat)> = Vec::new();
        for (year, data) in self.years_return.as_ref().unwrap() {
            let data: Vec<f64> = data.iter().map(|x| x.value).collect();

            let stat = Stock::cal_statistic(data);

            result.push((*year, stat));
        }

        result
    }

    pub fn stat_hold_year(&mut self) -> Vec<(i32, Stat)> {
        self.cal_return();

        let mut result: Vec<(i32, Stat)> = Vec::new();
        for (year, data) in self.years_return.as_ref().unwrap() {
            let end_date = data[data.len() - 1].end;
            let data: Vec<f64> = data
                .iter()
                .filter(|x| x.end == end_date)
                .map(|x| x.value)
                .collect();

            let stat = Stock::cal_statistic(data);

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

impl Stock {
    pub fn new_by_csv(symbol: &str, file_path: &str) -> Stock {
        let file = File::open(file_path).expect(&format!("開啟 {file_path} 失敗")[..]);
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

            let date = NaiveDate::parse_from_str(&record[0], "%Y-%m-%d")
                .unwrap_or_else(|_| panic!("price date {} 轉換失敗，格式為 %Y-%m-%d", &record[0]));
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
        stock
    }

    fn cal_return(&mut self) {
        if self.all_return.is_some() {
            return;
        }

        let mut result: Vec<Return> = Vec::new();
        let mut years_result: HashMap<i32, Vec<Return>> = HashMap::new();
        for (i, start) in self.data.iter().enumerate() {
            let start_year = start.date.year();
            for end in &self.data[i + 1..] {
                let r = Return {
                    start: start.date,
                    end: end.date,
                    value: (end.close_adj - start.close_adj) / start.close_adj,
                };
                if start_year == end.date.year() {
                    years_result.entry(start_year).or_default().push(r);
                }
                result.push(r);
            }
        }
        self.years_return = Some(years_result);
        self.all_return = Some(result);
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

    fn cal_statistic(data: Vec<f64>) -> Stat {
        let count = data.len();
        let mut data = statistics::Data::new(data);

        let mean = data.mean().unwrap();
        let std = data.std_dev().unwrap();
        let min = data.min();
        let max = data.max();

        let q1 = data.lower_quartile();
        let q2 = data.median();
        let q3 = data.upper_quartile();

        Stat {
            count,
            mean,
            std,
            min,
            q1,
            q2,
            q3,
            max,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use float_cmp::assert_approx_eq;

    #[test]
    fn test_read_csv() {
        let stock = Stock::new_by_csv("VTI", "tests//data.csv");
        assert_eq!(stock.symbol, "VTI");
        assert!(!stock.data.is_empty());
        println!("{:?}", stock.data[0]);
    }

    #[test]
    fn test_cal_return() {
        let mut stock = Stock::new_by_csv("VTI", "tests//data.csv");

        stock.cal_return();
        let result = &stock.all_return.unwrap();
        assert_eq!(result.len(), stock.data.len() * (stock.data.len() - 1) / 2);
        println!("{:?}", result[0]);
    }

    #[test]
    fn test_cal_years() {
        let mut stock = Stock::new_by_csv("VTI", "tests//data.csv");

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

    #[test]
    fn test_cal_statistic() {
        let mut stock = Stock::new_by_csv("VTI", "tests//data.csv");

        stock.cal_return();
        let data: Vec<f64> = stock.all_return.unwrap().iter().map(|x| x.value).collect();
        let stat = Stock::cal_statistic(data);
        dbg!(&stat);
        assert_eq!(stat.count, 4_048_435);
        assert_approx_eq!(f64, stat.mean, 0.700_680_347_180_011_5, epsilon = 0.0001);
        assert_approx_eq!(f64, stat.std, 0.650_469_698_879_045_9, epsilon = 0.0001);
        assert_approx_eq!(f64, stat.min, -0.350_002_858_888_694_26, epsilon = 0.0001);
        assert_approx_eq!(f64, stat.q1, 0.202_955_876_863_636_14, epsilon = 0.0001);
        assert_approx_eq!(f64, stat.q2, 0.519_365_946_505_478_3, epsilon = 0.0001);
        assert_approx_eq!(f64, stat.q3, 1.019_478_019_156_691_2, epsilon = 0.0001);
        assert_approx_eq!(f64, stat.max, 4.125_857_367_549_767, epsilon = 0.0001);
    }

    #[test]
    fn test_stat_active_all() {
        let mut stock = Stock::new_by_csv("VTI", "tests//data.csv");
        let stat = stock.stat_active_all();
        dbg!(&stat);
        assert_eq!(stat.count, 4_048_435);
        assert_approx_eq!(f64, stat.mean, 0.700_680_347_180_011_5, epsilon = 0.0001);
        assert_approx_eq!(f64, stat.std, 0.650_469_698_879_045_9, epsilon = 0.0001);
        assert_approx_eq!(f64, stat.min, -0.350_002_858_888_694_26, epsilon = 0.0001);
        assert_approx_eq!(f64, stat.q1, 0.202_955_876_863_636_14, epsilon = 0.0001);
        assert_approx_eq!(f64, stat.q2, 0.519_365_946_505_478_3, epsilon = 0.0001);
        assert_approx_eq!(f64, stat.q3, 1.019_478_019_156_691_2, epsilon = 0.0001);
        assert_approx_eq!(f64, stat.max, 4.125_857_367_549_767, epsilon = 0.0001);
    }

    #[test]
    fn test_stat_hold_all() {
        let mut stock = Stock::new_by_csv("VTI", "tests//data.csv");
        let stat = stock.stat_hold_all();
        dbg!(&stat);
        assert_eq!(stat.count, 2845);
        assert_approx_eq!(f64, stat.mean, 1.554_318_370_566_027_5, epsilon = 0.0001);
        assert_approx_eq!(f64, stat.std, 1.057_826_149_059_564_2, epsilon = 0.0001);
        assert_approx_eq!(
            f64,
            stat.min,
            0.000_276_306_700_437_496_07,
            epsilon = 0.0001
        );
        assert_approx_eq!(f64, stat.q1, 0.645_785_136_281_903, epsilon = 0.0001);
        assert_approx_eq!(f64, stat.q2, 1.277_502_831_019_586_6, epsilon = 0.0001);
        assert_approx_eq!(f64, stat.q3, 2.488_169_843_311_141, epsilon = 0.0001);
        assert_approx_eq!(f64, stat.max, 4.125_857_367_549_767, epsilon = 0.0001);
    }

    #[allow(clippy::too_many_lines)]
    #[test]
    fn test_stat_active_year() {
        let answer: HashMap<i32, Stat> = HashMap::from([
            (
                2010,
                Stat {
                    count: 31626,
                    mean: 0.037_017_844,
                    std: 0.073_642_899,
                    min: -0.162_290_081,
                    q1: -0.014_200_332,
                    q2: 0.033_401_081,
                    q3: 0.087_488_856,
                    max: 0.262_035_251,
                },
            ),
            (
                2011,
                Stat {
                    count: 31626,
                    mean: -0.030_776_179,
                    std: 0.062_213_648,
                    min: -0.203_041_106,
                    q1: -0.078_118_511,
                    q2: -0.027_276_818,
                    q3: 0.015_843_744,
                    max: 0.181_687_951,
                },
            ),
            (
                2012,
                Stat {
                    count: 31125,
                    mean: 0.032_247_184,
                    std: 0.044_807_779,
                    min: -0.100_589_879,
                    q1: 0.001_307_694,
                    q2: 0.031_167_747,
                    q3: 0.063_003_249,
                    max: 0.168_663_681,
                },
            ),
            (
                2013,
                Stat {
                    count: 31626,
                    mean: 0.082_614_458,
                    std: 0.064_634_988,
                    min: -0.056_941_96,
                    q1: 0.032_734_853,
                    q2: 0.072_361_398,
                    q3: 0.123_086_933,
                    max: 0.303_718_746,
                },
            ),
            (
                2014,
                Stat {
                    count: 31626,
                    mean: 0.045_463_069,
                    std: 0.042_984_553,
                    min: -0.075_662_874,
                    q1: 0.013_841_044,
                    q2: 0.042_985_845,
                    q3: 0.072_947_714,
                    max: 0.208_122_268,
                },
            ),
            (
                2015,
                Stat {
                    count: 31626,
                    mean: -0.004_289_436,
                    std: 0.037_263_305,
                    min: -0.120_610_684,
                    q1: -0.025_267_753,
                    q2: -0.002_141_399,
                    q3: 0.018_892_788,
                    max: 0.121_719_39,
                },
            ),
            (
                2016,
                Stat {
                    count: 31626,
                    mean: 0.066_271_663,
                    std: 0.060_238_885,
                    min: -0.101_097_457,
                    q1: 0.020_994_931,
                    q2: 0.056_136_5,
                    q3: 0.101_755_591,
                    max: 0.288_358_957,
                },
            ),
            (
                2017,
                Stat {
                    count: 31375,
                    mean: 0.057_284_63,
                    std: 0.044_796_072,
                    min: -0.027_144_263,
                    q1: 0.022_051_136,
                    q2: 0.048_613_165,
                    q3: 0.086_673_267,
                    max: 0.207_987_413,
                },
            ),
            (
                2018,
                Stat {
                    count: 31375,
                    mean: 0.006_675_123,
                    std: 0.054_403_281,
                    min: -0.200_464_711,
                    q1: -0.026_189_333,
                    q2: 0.010_968_156,
                    q3: 0.043_167_915,
                    max: 0.152_243_152,
                },
            ),
            (
                2019,
                Stat {
                    count: 31626,
                    mean: 0.059_549_264,
                    std: 0.056_488_337,
                    min: -0.067_848_31,
                    q1: 0.018_935_897,
                    q2: 0.049_833_745,
                    q3: 0.090_752_371,
                    max: 0.341_621_608,
                },
            ),
            (
                2020,
                Stat {
                    count: 31878,
                    mean: 0.104_785_725,
                    std: 0.152_938_804,
                    min: -0.350_002_885,
                    q1: 0.015_953_895,
                    q2: 0.086_201_042,
                    q3: 0.181_886_956,
                    max: 0.770_215_094,
                },
            ),
            (
                2021,
                Stat {
                    count: 2926,
                    mean: 0.031_905_695,
                    std: 0.032_138_786,
                    min: -0.049_640_954,
                    q1: 0.008_086_597,
                    q2: 0.030_221_072,
                    q3: 0.052_533_891,
                    max: 0.135_841_087,
                },
            ),
        ]);

        let mut stock = Stock::new_by_csv("VTI", "tests//data.csv");
        let stats = stock.stat_active_year();
        dbg!(&stats);
        for (ref y, stat) in stats {
            dbg!(&stat);
            assert_eq!(stat.count, answer.get(y).unwrap().count);
            assert_approx_eq!(
                f64,
                stat.mean,
                answer.get(y).unwrap().mean,
                epsilon = 0.0001
            );
            assert_approx_eq!(f64, stat.std, answer.get(y).unwrap().std, epsilon = 0.0001);
            assert_approx_eq!(f64, stat.min, answer.get(y).unwrap().min, epsilon = 0.0001);
            assert_approx_eq!(f64, stat.q1, answer.get(y).unwrap().q1, epsilon = 0.0001);
            assert_approx_eq!(f64, stat.q2, answer.get(y).unwrap().q2, epsilon = 0.0001);
            assert_approx_eq!(f64, stat.q3, answer.get(y).unwrap().q3, epsilon = 0.0001);
            assert_approx_eq!(f64, stat.max, answer.get(y).unwrap().max, epsilon = 0.0001);
        }
    }

    #[allow(clippy::too_many_lines)]
    #[test]
    fn test_stat_hold_year() {
        let answer = HashMap::from([
            (
                2010,
                Stat {
                    count: 251,
                    mean: 0.130_642_784_481_965_15,
                    std: 0.061_892_950_507_839_994,
                    min: -0.001_691_386_968_915_610_7,
                    q1: 0.079_980_454_411_885_6,
                    q2: 0.139_160_615_643_549_4,
                    q3: 0.177_548_883_773_924_97,
                    max: 0.259_900_696_985_393_75,
                },
            ),
            (
                2011,
                Stat {
                    count: 251,
                    mean: -0.001_943_151_825_438_86,
                    std: 0.052_350_032_188_198_61,
                    min: -0.076_582_330_934_112,
                    q1: -0.043_083_283_976_492_02,
                    q2: -0.012_816_195_386_285_312,
                    q3: 0.035_066_044_580_685_32,
                    max: 0.158_676_675_746_498_27,
                },
            ),
            (
                2012,
                Stat {
                    count: 249,
                    mean: 0.051_032_358_336_657_78,
                    std: 0.039_721_600_321_106_366,
                    min: -0.017_456_557_660_954_497,
                    q1: 0.020_583_618_757_437_6,
                    q2: 0.046_580_197_084_146_09,
                    q3: 0.077_901_134_103_444_07,
                    max: 0.148_262_838_293_110_68,
                },
            ),
            (
                2013,
                Stat {
                    count: 251,
                    mean: 0.145_962_046_348_747_57,
                    std: 0.078_593_859_085_474_05,
                    min: 0.003_347_252_371_354_567,
                    q1: 0.092_580_175_240_918_52,
                    q2: 0.139_771_552_701_751_9,
                    q3: 0.210_129_533_470_873_24,
                    max: 0.303_718_746_527_564_7,
                },
            ),
            (
                2014,
                Stat {
                    count: 251,
                    mean: 0.073_435_766_182_353_17,
                    std: 0.045_286_184_933_266_65,
                    min: -0.014_778_014_213_744_592,
                    q1: 0.039_225_903_069_609_08,
                    q2: 0.068_251_153_003_633_07,
                    q3: 0.108_953_760_637_612_49,
                    max: 0.190_268_600_545_890_62,
                },
            ),
            (
                2015,
                Stat {
                    count: 251,
                    mean: -0.008_161_155_619_217_51,
                    std: 0.027_093_289_587_183_378,
                    min: -0.043_709_093_724_268_93,
                    q1: -0.028_254_657_618_029_186,
                    q2: -0.018_633_936_559_505_373,
                    q3: 0.005_843_712_616_327_796,
                    max: 0.087_448_868_926_023_74,
                },
            ),
            (
                2016,
                Stat {
                    count: 251,
                    mean: 0.092_854_736_745_804_62,
                    std: 0.065_770_070_178_796_67,
                    min: -0.013_262_745_798_817_38,
                    q1: 0.045_430_470_805_241_23,
                    q2: 0.084_553_370_378_690_19,
                    q3: 0.123_947_369_950_619_85,
                    max: 0.271_271_736_431_458_97,
                },
            ),
            (
                2017,
                Stat {
                    count: 250,
                    mean: 0.105_127_511_080_716_38,
                    std: 0.053_135_083_472_993_51,
                    min: -0.004_166_126_647_417_749,
                    q1: 0.064_414_241_492_289_41,
                    q2: 0.110_070_737_788_701_83,
                    q3: 0.146_901_013_547_388_2,
                    max: 0.202_954_795_965_269_72,
                },
            ),
            (
                2018,
                Stat {
                    count: 250,
                    mean: -0.084_226_792_765_933_9,
                    std: 0.035_768_882_752_304_96,
                    min: -0.147_496_295_204_501_7,
                    q1: -0.108_042_968_175_411_66,
                    q2: -0.082_825_105_092_926_36,
                    q3: -0.061_566_737_276_832_03,
                    max: 0.066_248_992_176_016_22,
                },
            ),
            (
                2019,
                Stat {
                    count: 251,
                    mean: 0.116_692_700_501_130_24,
                    std: 0.061_193_915_171_779_09,
                    min: -0.003_714_360_544_244_795,
                    q1: 0.079_292_178_435_305_03,
                    q2: 0.110_934_373_264_646_25,
                    q3: 0.150_670_189_802_067_2,
                    max: 0.336_638_297_879_440_33,
                },
            ),
            (
                2020,
                Stat {
                    count: 252,
                    mean: 0.220_279_204_001_704_78,
                    std: 0.148_704_611_772_618_82,
                    min: 0.003_092_263_977_986_637_5,
                    q1: 0.129_240_044_169_774_4,
                    q2: 0.189_051_220_994_654_27,
                    q3: 0.284_941_656_343_590_64,
                    max: 0.770_215_148_973_804_6,
                },
            ),
            (
                2021,
                Stat {
                    count: 76,
                    mean: 0.065_028_855_939_338_46,
                    std: 0.032_195_098_802_457_45,
                    min: 0.000_276_306_700_437_496_07,
                    q1: 0.050_524_023_574_706_98,
                    q2: 0.064_860_829_308_661_74,
                    q3: 0.088_440_386_445_859_79,
                    max: 0.135_841_069_892_111_56,
                },
            ),
        ]);

        let mut stock = Stock::new_by_csv("VTI", "tests//data.csv");
        let stats = stock.stat_hold_year();
        dbg!(&stats);
        for (ref y, stat) in stats {
            dbg!(&stat);
            assert_eq!(stat.count, answer.get(y).unwrap().count);
            assert_approx_eq!(
                f64,
                stat.mean,
                answer.get(y).unwrap().mean,
                epsilon = 0.0001
            );
            assert_approx_eq!(f64, stat.std, answer.get(y).unwrap().std, epsilon = 0.0001);
            assert_approx_eq!(f64, stat.min, answer.get(y).unwrap().min, epsilon = 0.0001);
            assert_approx_eq!(f64, stat.q1, answer.get(y).unwrap().q1, epsilon = 0.0001);
            assert_approx_eq!(f64, stat.q2, answer.get(y).unwrap().q2, epsilon = 0.0001);
            assert_approx_eq!(f64, stat.q3, answer.get(y).unwrap().q3, epsilon = 0.0001);
            assert_approx_eq!(f64, stat.max, answer.get(y).unwrap().max, epsilon = 0.0001);
        }
    }

    #[test]
    fn test_cal_years_return() {
        let answer: HashMap<i32, f64> = HashMap::from([
            (2010, 0.155_022_020_360_802_15),
            (2011, -0.000_626_811_654_388_168_3),
            (2012, 0.148_262_838_293_110_68),
            (2013, 0.301_463_224_718_343_5),
            (2014, 0.135_437_206_406_264_67),
            (2015, 0.004_308_299_308_666_056),
            (2016, 0.145_307_737_774_449_83),
            (2017, 0.202_954_795_965_269_72),
            (2018, -0.058_998_388_264_985_21),
            (2019, 0.305_663_115_103_234_85),
            (2020, 0.200_780_520_909_719_35),
            (2021, 0.135_841_069_892_111_56),
        ]);
        let mut stock = Stock::new_by_csv("VTI", "tests//data.csv");
        let rs = stock.cal_years_return();
        dbg!(&rs);
        for (ref y, r) in rs {
            assert_approx_eq!(f64, r, *answer.get(y).unwrap());
        }
    }

    #[test]
    fn test_n_years() {
        let stock = Stock::new_by_csv("VTI", "tests//data.csv");
        assert_eq!(stock.n_years(), 12);
    }
}
