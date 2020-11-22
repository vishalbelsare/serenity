import fire
import quandl

from serenity.equity.sharadar_api import init_quandl, create_sharadar_session, UnitType, Indicator


def load_sharadar_refdata():
    init_quandl()
    session = create_sharadar_session()
    load_indicators(session)
    session.commit()


def load_indicators(session):
    df = quandl.get_table("SHARADAR/INDICATORS", paginate=True)
    for index, row in df.iterrows():
        table_name, indicator, is_filter, is_primary_key, title, description, unit_type_code = row
        if is_filter == 'N':
            is_filter = False
        else:
            is_filter = True
        if is_primary_key == 'N':
            is_primary_key = False
        else:
            is_primary_key = True

        unit_type = UnitType.find_by_code(session, unit_type_code)
        if unit_type is None:
            unit_type = UnitType(unit_type_code)
            session.add(unit_type)

        ind_entity = Indicator.find_by_name(session, table_name, indicator)
        if ind_entity is None:
            ind_entity = Indicator(table_name=table_name,
                                   indicator=indicator,
                                   is_filter=is_filter,
                                   is_primary_key=is_primary_key,
                                   title=title,
                                   description=description,
                                   unit_type=unit_type)
            session.add(ind_entity)


if __name__ == '__main__':
    fire.Fire(load_sharadar_refdata)


