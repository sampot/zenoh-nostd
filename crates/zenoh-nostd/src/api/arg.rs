use core::marker::PhantomData;

use crate::{api::query::QueryableQuery, config::ZSessionConfig};

use super::{liveliness::LivelinessEvent, response::*, sample::*};

pub trait ZArg {
    type Of<'a>
    where
        Self: 'a;
}

pub struct GetResponseRef;
pub struct SampleRef;
pub struct QueryableQueryRef<'res, Config>(PhantomData<&'res Config>);
// asrun P3: liveliness 事件以引用交付(ke 借自 rx buffer,回呼期間有效)
pub struct LivelinessArg;

impl ZArg for GetResponseRef {
    type Of<'a> = &'a GetResponse<'a>;
}

impl ZArg for SampleRef {
    type Of<'a> = &'a Sample<'a>;
}

impl ZArg for LivelinessArg {
    type Of<'a> = &'a LivelinessEvent<'a>;
}

impl<'res, Config> ZArg for QueryableQueryRef<'res, Config>
where
    Config: ZSessionConfig,
{
    type Of<'a>
        = &'a QueryableQuery<'a, 'res, Config>
    where
        Self: 'a;
}
