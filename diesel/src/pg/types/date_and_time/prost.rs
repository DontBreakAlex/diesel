use super::PgTimestamp;
use crate::deserialize::{self, FromSql};
use crate::pg::{Pg, PgValue};
use crate::serialize::{self, Output, ToSql};
use crate::sql_types::{Timestamp, Timestamptz};

#[cfg(all(feature = "prost", feature = "postgres_backend"))]
impl FromSql<Timestamp, Pg> for prost_types::Timestamp {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        let PgTimestamp(offset) = FromSql::<Timestamp, Pg>::from_sql(bytes)?;

        if offset >= -62135596800000 && offset <= 253402300799999 {
            Ok(prost_types::Timestamp {
                seconds: offset / 1_000_000,
                nanos: ((offset % 1_000_000) * 1000) as i32,
            })
        } else {
            let message = "Tried to deserialize a timestamp that is too large for protobuf";
            Err(message.into())
        }
    }
}

#[cfg(all(feature = "prost", feature = "postgres_backend"))]
impl ToSql<Timestamp, Pg> for prost_types::Timestamp {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        // The range of a protobuf timestamp is from 0001-01-01T00:00:00Z to 9999-12-31T23:59:59.999999999Z,
        // therefore, it will fit in a postgres timestamp.
        let microseconds = self.seconds * 1_000_000 + self.nanos as i64 / 1000;

        ToSql::<Timestamp, Pg>::to_sql(&PgTimestamp(microseconds), &mut out.reborrow())
    }
}

#[cfg(all(feature = "prost", feature = "postgres_backend"))]
impl FromSql<Timestamptz, Pg> for prost_types::Timestamp {
    fn from_sql(bytes: PgValue<'_>) -> deserialize::Result<Self> {
        FromSql::<Timestamp, Pg>::from_sql(bytes)
    }
}

#[cfg(all(feature = "prost", feature = "postgres_backend"))]
impl ToSql<Timestamptz, Pg> for prost_types::Timestamp {
    fn to_sql<'b>(&'b self, out: &mut Output<'b, '_, Pg>) -> serialize::Result {
        ToSql::<Timestamp, Pg>::to_sql(self, out)
    }
}

#[cfg(test)]
mod tests {
    use crate::dsl::sql;
    use crate::prelude::*;
    use crate::select;
    use crate::sql_types::Timestamp;
    use crate::test_helpers::connection;

    #[test]
    fn unix_epoch_encodes_correctly() {
        let connection = &mut connection();
        let time = prost_types::Timestamp {
            seconds: 0,
            nanos: 0,
        };
        let query = select(sql::<Timestamp>("'1970-01-01'").eq(time));
        assert!(query.get_result::<bool>(connection).unwrap());
    }

    #[test]
    fn unix_epoch_decodes_correctly() {
        let connection = &mut connection();
        let time = prost_types::Timestamp {
            seconds: 0,
            nanos: 0,
        };
        let epoch_from_sql =
            select(sql::<Timestamp>("'1970-01-01'::timestamp")).get_result(connection);
        assert_eq!(Ok(time), epoch_from_sql);
    }

    #[test]
    fn invalid_ranges_fail() {
        let connection = &mut connection();
        let query = select(sql::<Timestamp>("'10001-01-01'"));
        let res: Result<prost_types::Timestamp, crate::result::Error> = query.get_result(connection);

        assert!(res.is_err());
    }
}