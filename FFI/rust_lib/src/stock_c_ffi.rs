use crate::stock;
use libc::size_t;
use std::convert::From;
use std::slice;

// A struct that can be passed between C and Rust
#[derive(Debug)]
#[repr(C)]
pub struct Stat {
    year: i32,
    count: usize,
    mean: f64,
    std: f64,
    min: f64,
    q1: f64,
    q2: f64,
    q3: f64,
    max: f64,
}

impl From<stock::Stat> for Stat {
    fn from(stat: stock::Stat) -> Self {
        Stat {
            year: -1,
            count: stat.count,
            mean: stat.mean,
            std: stat.std,
            min: stat.min,
            q1: stat.q1,
            q2: stat.q2,
            q3: stat.q3,
            max: stat.max,
        }
    }
}

// A struct that can be passed between C and Rust
#[derive(Debug)]
#[repr(C)]
pub struct Price {
    pub date: *const libc::c_char,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub close_adj: f64,
    pub volume: u64,
}

// A struct that can be passed between C and Rust
#[derive(Debug)]
#[repr(C)]
pub struct Return {
    year: i32,
    value: f64,
}

#[no_mangle]
pub extern "C" fn stock_new(data: *const Price, data_len: size_t) -> *mut stock::Stock {
    let data = unsafe {
        assert!(!data.is_null());

        slice::from_raw_parts(data, data_len)
    };
    let data: Vec<stock::Price> = data.iter().map(|x| x.into()).collect();

    //println!("get: {:?}", data);

    match stock::Stock::new(&data) {
        Ok(stock) => Box::into_raw(Box::new(stock)),
        Err(e) => panic!("{}", e),
    }
}

#[no_mangle]
pub extern "C" fn stock_free(ptr: *mut stock::Stock) {
    if ptr.is_null() {
        return;
    }
    unsafe {
        let _ = Box::from_raw(ptr);
    }
}

#[no_mangle]
pub extern "C" fn stock_stat_active_all(ptr: *mut stock::Stock) -> Stat {
    let stock = unsafe {
        assert!(!ptr.is_null());
        &mut *ptr
    };
    let stat = stock.stat_active_all();

    stat.into()
}

#[no_mangle]
pub extern "C" fn stock_stat_hold_all(ptr: *mut stock::Stock) -> Stat {
    let stock = unsafe {
        assert!(!ptr.is_null());
        &mut *ptr
    };
    let stat = stock.stat_hold_all();

    stat.into()
}

#[no_mangle]
pub extern "C" fn stock_n_years(ptr: *mut stock::Stock) -> usize {
    let stock = unsafe {
        assert!(!ptr.is_null());
        &mut *ptr
    };
    stock.n_years()
}

#[no_mangle]
pub extern "C" fn stock_stat_active_year(
    ptr: *mut stock::Stock,
    result: *mut Stat,
    result_len: size_t,
) {
    let stock = unsafe {
        assert!(!ptr.is_null());
        &mut *ptr
    };

    let data = unsafe {
        assert!(!result.is_null());

        slice::from_raw_parts_mut(result, result_len)
    };

    let stats = stock.stat_active_year();
    for (i, (year, stat)) in stats.iter().enumerate() {
        data[i].year = *year;
        data[i].count = stat.count;
        data[i].mean = stat.mean;
        data[i].std = stat.std;
        data[i].min = stat.min;
        data[i].q1 = stat.q1;
        data[i].q2 = stat.q2;
        data[i].q3 = stat.q3;
        data[i].max = stat.max;
    }
}
#[no_mangle]
pub extern "C" fn stock_stat_hold_year(
    ptr: *mut stock::Stock,
    result: *mut Stat,
    result_len: size_t,
) {
    let stock = unsafe {
        assert!(!ptr.is_null());
        &mut *ptr
    };

    let data = unsafe {
        assert!(!result.is_null());

        slice::from_raw_parts_mut(result, result_len)
    };

    let stats = stock.stat_hold_year();
    for (i, (year, stat)) in stats.iter().enumerate() {
        data[i].year = *year;
        data[i].count = stat.count;
        data[i].mean = stat.mean;
        data[i].std = stat.std;
        data[i].min = stat.min;
        data[i].q1 = stat.q1;
        data[i].q2 = stat.q2;
        data[i].q3 = stat.q3;
        data[i].max = stat.max;
    }
}

#[no_mangle]
pub extern "C" fn stock_year_return(
    ptr: *mut stock::Stock,
    result: *mut Return,
    result_len: size_t,
) {
    let stock = unsafe {
        assert!(!ptr.is_null());
        &mut *ptr
    };

    let data = unsafe {
        assert!(!result.is_null());

        slice::from_raw_parts_mut(result, result_len)
    };

    let vals = stock.cal_years_return();
    for (i, (year, r)) in vals.iter().enumerate() {
        data[i].year = *year;
        data[i].value = *r;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_stat_into() {
        let s = stock::Stat {
            count: 1,
            mean: 1.0,
            std: 1.0,
            min: 1.0,
            q1: 1.0,
            q2: 1.0,
            q3: 1.0,
            max: 1.0,
        };
        let s: Stat = s.into();
        assert_eq!(s.year, -1);
    }
}
