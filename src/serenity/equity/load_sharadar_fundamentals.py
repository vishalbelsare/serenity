import fire
import pandas as pd
import quandl

from serenity.equity.sharadar_api import init_quandl, create_sharadar_session
from serenity.equity.sharadar_fundamentals import DimensionType, Fundamentals
from serenity.equity.sharadar_refdata import Ticker


def backfill_sharadar_fundamentals():
    init_quandl()
    session = create_sharadar_session()

    load_path = 'sharadar_fundamentals.zip'
    # quandl.export_table('SHARADAR/SF1', filename=load_path)
    df = pd.read_csv(load_path)
    row_count = 0
    for index, row in df.iterrows():
        ticker_code = row['ticker']
        ticker = Ticker.find_by_ticker(session, ticker_code)
        if ticker is None:
            continue

        dimension_type_code = row['dimension']
        dimension_type = DimensionType.get_or_create(session, dimension_type_code)

        calendar_date = row['calendardate']
        date_key = row['datekey']
        report_period = row['reportperiod']
        last_updated = row['lastupdated']

        accoci = clean_nan(row['accoci'])
        assets = clean_nan(row['assets'])
        assets_avg = clean_nan(row['assetsavg'])
        assets_c = clean_nan(row['assetsc'])
        assets_nc = clean_nan(row['assetsnc'])
        asset_turnover = clean_nan(row['assetturnover'])
        bvps = clean_nan(row['bvps'])
        capex = clean_nan(row['capex'])
        cash_neq = clean_nan(row['cashneq'])
        cash_neq_usd = clean_nan(row['cashnequsd'])
        cor = clean_nan(row['cor'])
        consol_inc = clean_nan(row['consolinc'])
        current_ratio = clean_nan(row['currentratio'])
        de = clean_nan(row['de'])
        debt = clean_nan(row['debt'])
        debt_c = clean_nan(row['debtc'])
        debt_nc = clean_nan(row['debtnc'])
        debt_usd = clean_nan(row['debtusd'])
        deferred_rev = clean_nan(row['deferredrev'])
        dep_amor = clean_nan(row['depamor'])
        deposits = clean_nan(row['deposits'])
        div_yield = clean_nan(row['divyield'])
        dps = clean_nan(row['dps'])
        ebit = clean_nan(row['ebit'])
        ebitda = clean_nan(row['ebitda'])
        ebitda_margin = clean_nan(row['ebitdamargin'])
        ebitda_usd = clean_nan(row['ebitdausd'])
        ebit_usd = clean_nan(row['ebitusd'])
        ebt = clean_nan(row['ebt'])
        eps = clean_nan(row['eps'])
        eps_dil = clean_nan(row['epsdil'])
        eps_usd = clean_nan(row['epsusd'])
        equity = clean_nan(row['equity'])
        equity_avg = clean_nan(row['equityavg'])
        equity_usd = clean_nan(row['equityusd'])
        ev = clean_nan(row['ev'])
        ev_ebit = clean_nan(row['evebit'])
        ev_ebitda = clean_nan(row['evebitda'])
        fcf = clean_nan(row['fcf'])
        fcf_ps = clean_nan(row['fcfps'])
        fx_usd = clean_nan(row['fxusd'])
        gp = clean_nan(row['gp'])
        gross_margin = clean_nan(row['grossmargin'])
        int_exp = clean_nan(row['intexp'])
        inv_cap = clean_nan(row['invcap'])
        inv_cap_avg = clean_nan(row['invcapavg'])
        inventory = clean_nan(row['inventory'])
        investments = clean_nan(row['investments'])
        investments_c = clean_nan(row['investmentsc'])
        investments_nc = clean_nan(row['investmentsnc'])
        liabilities = clean_nan(row['liabilities'])
        liabilities_c = clean_nan(row['liabilitiesc'])
        liabilities_nc = clean_nan(row['liabilitiesnc'])
        market_cap = clean_nan(row['marketcap'])
        ncf = clean_nan(row['ncf'])
        ncf_bus = clean_nan(row['ncfbus'])
        ncf_common = clean_nan(row['ncfcommon'])
        ncf_debt = clean_nan(row['ncfdebt'])
        ncf_div = clean_nan(row['ncfdiv'])
        ncf_f = clean_nan(row['ncff'])
        ncf_i = clean_nan(row['ncfi'])
        ncf_inv = clean_nan(row['ncfinv'])
        ncf_o = clean_nan(row['ncfo'])
        ncf_x = clean_nan(row['ncfx'])
        net_inc = clean_nan(row['netinc'])
        net_inc_cmn = clean_nan(row['netinccmn'])
        net_inc_cmn_usd = clean_nan(row['netinccmnusd'])
        net_inc_dis = clean_nan(row['netincdis'])
        net_inc_nci = clean_nan(row['netincnci'])
        net_margin = clean_nan(row['netmargin'])
        op_ex = clean_nan(row['opex'])
        op_inc = clean_nan(row['opinc'])
        payables = clean_nan(row['payables'])
        payout_ratio = clean_nan(row['payoutratio'])
        pb = clean_nan(row['pb'])
        pe = clean_nan(row['pe'])
        pe1 = clean_nan(row['pe1'])
        ppne_net = clean_nan(row['ppnenet'])
        pref_div_is = clean_nan(row['prefdivis'])
        price = clean_nan(row['price'])
        ps = clean_nan(row['ps'])
        ps1 = clean_nan(row['ps1'])
        receivables = clean_nan(row['receivables'])
        ret_earn = clean_nan(row['retearn'])
        revenue = clean_nan(row['revenue'])
        revenue_usd = clean_nan(row['revenueusd'])
        rnd = clean_nan(row['rnd'])
        roa = clean_nan(row['roa'])
        roe = clean_nan(row['roe'])
        roic = clean_nan(row['roic'])
        ros = clean_nan(row['ros'])
        sb_comp = clean_nan(row['sbcomp'])
        sgna = clean_nan(row['sgna'])
        share_factor = clean_nan(row['sharefactor'])
        shares_bas = clean_nan(row['sharesbas'])
        shares_wa = clean_nan(row['shareswa'])
        shares_wa_dil = clean_nan(row['shareswadil'])
        sps = clean_nan(row['sps'])
        tangibles = clean_nan(row['tangibles'])
        tax_assets = clean_nan(row['taxassets'])
        tax_exp = clean_nan(row['taxexp'])
        tax_liabilities = clean_nan(row['taxliabilities'])
        tbvps = clean_nan(row['tbvps'])
        working_capital = clean_nan(row['workingcapital'])

        fundamentals = Fundamentals(ticker=ticker, dimension_type=dimension_type, calendar_date=calendar_date,
                                    date_key=date_key, report_period=report_period, last_updated=last_updated,
                                    accoci=accoci, assets=assets, assets_avg=assets_avg, assets_c=assets_c,
                                    assets_nc=assets_nc, asset_turnover=asset_turnover, bvps=bvps, capex=capex,
                                    cash_neq=cash_neq, cash_neq_usd=cash_neq_usd, cor=cor, consol_inc=consol_inc,
                                    current_ratio=current_ratio, de=de, debt=debt, debt_c=debt_c, debt_nc=debt_nc,
                                    debt_usd=debt_usd, deferred_rev=deferred_rev, dep_amor=dep_amor, deposits=deposits,
                                    div_yield=div_yield, dps=dps, ebit=ebit, ebitda=ebitda, ebitda_margin=ebitda_margin,
                                    ebitda_usd=ebitda_usd, ebit_usd=ebit_usd, ebt=ebt, eps=eps, eps_dil=eps_dil,
                                    eps_usd=eps_usd, equity=equity, equity_avg=equity_avg, equity_usd=equity_usd,
                                    ev=ev, ev_ebit=ev_ebit, ev_ebitda=ev_ebitda, fcf=fcf, fcf_ps=fcf_ps, fx_usd=fx_usd,
                                    gp=gp, gross_margin=gross_margin, int_exp=int_exp, inv_cap=inv_cap,
                                    inv_cap_avg=inv_cap_avg, inventory=inventory, investments=investments,
                                    investments_c=investments_c, investments_nc=investments_nc, liabilities=liabilities,
                                    liabilities_c=liabilities_c, liabilities_nc=liabilities_nc, market_cap=market_cap,
                                    ncf=ncf, ncf_bus=ncf_bus, ncf_common=ncf_common, ncf_debt=ncf_debt, ncf_div=ncf_div,
                                    ncf_f=ncf_f, ncf_i=ncf_i, ncf_inv=ncf_inv, ncf_o=ncf_o, ncf_x=ncf_x,
                                    net_inc=net_inc, net_inc_cmn=net_inc_cmn, net_inc_cmn_usd=net_inc_cmn_usd,
                                    net_inc_dis=net_inc_dis, net_inc_nci=net_inc_nci, net_margin=net_margin,
                                    op_ex=op_ex, op_inc=op_inc, payables=payables, payout_ratio=payout_ratio, pb=pb,
                                    pe=pe, pe1=pe1, ppne_net=ppne_net, pref_div_is=pref_div_is, price=price, ps=ps,
                                    ps1=ps1, receivables=receivables, ret_earn=ret_earn, revenue=revenue,
                                    revenue_usd=revenue_usd, rnd=rnd, roa=roa, roe=roe, roic=roic, ros=ros,
                                    sb_comp=sb_comp, sgna=sgna, share_factor=share_factor, shares_bas=shares_bas,
                                    shares_wa=shares_wa, shares_wa_dil=shares_wa_dil, sps=sps, tangibles=tangibles,
                                    tax_assets=tax_assets, tax_exp=tax_exp, tax_liabilities=tax_liabilities,
                                    tbvps=tbvps, working_capital=working_capital)
        session.add(fundamentals)

        if row_count % 1000 == 0:
            session.commit()
        row_count += 1

    session.commit()


def clean_nan(value):
    if pd.isnull(value):
        return None
    else:
        return value


if __name__ == '__main__':
    fire.Fire(backfill_sharadar_fundamentals)
