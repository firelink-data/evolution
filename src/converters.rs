use crate::slicers::FnLineBreak;

pub(crate) mod convert_to_self;
pub(crate) mod convert_to_arrow;


pub(crate) trait Converter {
    fn set_line_break_handler(&mut self, fn_line_break: FnLineBreak);
    fn get_line_break_handler(&self) -> FnLineBreak;

    fn process(&mut self, slices: Vec<&[u8]>) -> usize;
}
