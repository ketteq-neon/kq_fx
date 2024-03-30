pub fn calculate_page_size(first_date: i32, last_date: i32, entry_count: i32) -> i32 {
    let date_range = last_date - first_date;
    let avg_entries_per_week_calendar = date_range as f64 / 7.0;
    let entry_count_d = entry_count as f64;
    let mut page_size_tmp = 32; // monthly calendar

    if entry_count_d > avg_entries_per_week_calendar {
        page_size_tmp = 16; // weekly calendar
    }

    page_size_tmp
}