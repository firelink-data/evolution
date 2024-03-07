use crate::slicers::FnLineBreak;

pub(crate) mod self_converter;
pub(crate) mod arrow2_converter;

pub mod arrow_converter;



pub(crate) trait Converter<'a> {
    fn set_line_break_handler(& mut self, fn_line_break: FnLineBreak<'a>);
    fn get_line_break_handler(& self) -> FnLineBreak<'a>;

    fn process(& mut self, slices: Vec< &[u8]>) -> usize;
}

pub trait ColumnBuilder {
    fn parse_value(&mut self, name: &str);
    fn lenght_in_chars(&mut self) -> i16;

}

