use criterion::{black_box, criterion_group, criterion_main, Criterion};

use fintech::stock::Stock;

fn benchmark_read_csv(c: &mut Criterion) {
    c.bench_function("read_csv VTI", |b| {
        b.iter(|| Stock::new_by_csv(black_box("VTI"), black_box("tests//data.csv")))
    });
}

// fn benchmark_cal_return(c: &mut Criterion) {
//     let stock = Stock::new_by_csv("VTI", "tests//data.csv");
//     match stock {
//         Ok(mut stock) => {
//             c.bench_function("cal_return", |b| b.iter(|| stock.cal_return()));
//         }
//         Err(e) => panic!("{}", e),
//     }
// }

// fn benchmark_cal_years(c: &mut Criterion) {
//     let stock = Stock::new_by_csv("VTI", "tests//data.csv");
//     match stock {
//         Ok(mut stock) => {
//             c.bench_function("cal_years", |b| b.iter(|| stock.cal_years()));
//         }
//         Err(e) => panic!("{}", e),
//     }
// }

// fn benchmark_cal_statistic(c: &mut Criterion) {
//     let stock = Stock::new_by_csv("VTI", "tests//data.csv");
//     match stock {
//         Ok(mut stock) => {
//             stock.cal_return();
//             let data: Vec<f64> = stock.all_return.unwrap().iter().map(|x| x.value).collect();
//             c.bench_function("cal_statistic", |b| b.iter(|| Stock::cal_statistic(&data[..])));
//         }
//         Err(e) => panic!("{}", e),
//     }
// }

fn benchmark_stat_active_all(c: &mut Criterion) {
    let stock = Stock::new_by_csv("VTI", "tests//data.csv");
    match stock {
        Ok(mut stock) => {
            c.bench_function("stat_active_all", |b| b.iter(|| stock.stat_active_all()));
        }
        Err(e) => panic!("{}", e),
    }
}

fn benchmark_stat_hold_all(c: &mut Criterion) {
    let stock = Stock::new_by_csv("VTI", "tests//data.csv");
    match stock {
        Ok(mut stock) => {
            c.bench_function("stat_hold_all", |b| b.iter(|| stock.stat_hold_all()));
        }
        Err(e) => panic!("{}", e),
    }
}

fn benchmark_stat_active_year(c: &mut Criterion) {
    let stock = Stock::new_by_csv("VTI", "tests//data.csv");
    match stock {
        Ok(mut stock) => {
            c.bench_function("stat_active_year", |b| b.iter(|| stock.stat_active_year()));
        }
        Err(e) => panic!("{}", e),
    }
}

fn benchmark_stat_hold_year(c: &mut Criterion) {
    let stock = Stock::new_by_csv("VTI", "tests//data.csv");
    match stock {
        Ok(mut stock) => {
            c.bench_function("stat_hold_year", |b| b.iter(|| stock.stat_hold_year()));
        }
        Err(e) => panic!("{}", e),
    }
}

fn benchmark_cal_years_return(c: &mut Criterion) {
    let stock = Stock::new_by_csv("VTI", "tests//data.csv");
    match stock {
        Ok(mut stock) => {
            c.bench_function("cal_years_return", |b| b.iter(|| stock.cal_years_return()));
        }
        Err(e) => panic!("{}", e),
    }
}

criterion_group!(
    benches,
    benchmark_read_csv,
    benchmark_stat_active_all,
    benchmark_stat_hold_all,
    benchmark_stat_active_year,
    benchmark_stat_hold_year,
    benchmark_cal_years_return
);
criterion_main!(benches);
