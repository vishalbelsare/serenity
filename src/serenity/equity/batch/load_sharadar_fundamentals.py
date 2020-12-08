import datetime

import luigi

from serenity.equity.batch.utils import LoadSharadarTableTask, ExportQuandlTableTask
from serenity.equity.sharadar_api import clean_nulls
from serenity.equity.sharadar_fundamentals import DimensionType, Fundamentals
from serenity.equity.sharadar_refdata import Ticker


class LoadEquityFundamentalsTask(LoadSharadarTableTask):
    start_date = luigi.DateParameter(default=datetime.date.today())
    end_date = luigi.DateParameter(default=datetime.date.today())

    def requires(self):
        yield ExportQuandlTableTask(table_name='SHARADAR/SF1', date_column='lastupdated',
                                    start_date=self.start_date, end_date=self.end_date)

    def process_row(self, index, row):
        ticker_code = row['ticker']
        ticker = Ticker.find_by_ticker(self.session, ticker_code)
        if ticker is None:
            self.logger.warning(f'unknown ticker referenced; skipping: {ticker_code}')
            return

        dimension_type_code = row['dimension']
        dimension_type = DimensionType.get_or_create(self.session, dimension_type_code)

        calendar_date = row['calendardate']
        date_key = row['datekey']
        report_period = row['reportperiod']
        last_updated = row['lastupdated']

        accoci = clean_nulls(row['accoci'])
        assets = clean_nulls(row['assets'])
        assets_avg = clean_nulls(row['assetsavg'])
        assets_c = clean_nulls(row['assetsc'])
        assets_nc = clean_nulls(row['assetsnc'])
        asset_turnover = clean_nulls(row['assetturnover'])
        bvps = clean_nulls(row['bvps'])
        capex = clean_nulls(row['capex'])
        cash_neq = clean_nulls(row['cashneq'])
        cash_neq_usd = clean_nulls(row['cashnequsd'])
        cor = clean_nulls(row['cor'])
        consol_inc = clean_nulls(row['consolinc'])
        current_ratio = clean_nulls(row['currentratio'])
        de = clean_nulls(row['de'])
        debt = clean_nulls(row['debt'])
        debt_c = clean_nulls(row['debtc'])
        debt_nc = clean_nulls(row['debtnc'])
        debt_usd = clean_nulls(row['debtusd'])
        deferred_rev = clean_nulls(row['deferredrev'])
        dep_amor = clean_nulls(row['depamor'])
        deposits = clean_nulls(row['deposits'])
        div_yield = clean_nulls(row['divyield'])
        dps = clean_nulls(row['dps'])
        ebit = clean_nulls(row['ebit'])
        ebitda = clean_nulls(row['ebitda'])
        ebitda_margin = clean_nulls(row['ebitdamargin'])
        ebitda_usd = clean_nulls(row['ebitdausd'])
        ebit_usd = clean_nulls(row['ebitusd'])
        ebt = clean_nulls(row['ebt'])
        eps = clean_nulls(row['eps'])
        eps_dil = clean_nulls(row['epsdil'])
        eps_usd = clean_nulls(row['epsusd'])
        equity = clean_nulls(row['equity'])
        equity_avg = clean_nulls(row['equityavg'])
        equity_usd = clean_nulls(row['equityusd'])
        ev = clean_nulls(row['ev'])
        ev_ebit = clean_nulls(row['evebit'])
        ev_ebitda = clean_nulls(row['evebitda'])
        fcf = clean_nulls(row['fcf'])
        fcf_ps = clean_nulls(row['fcfps'])
        fx_usd = clean_nulls(row['fxusd'])
        gp = clean_nulls(row['gp'])
        gross_margin = clean_nulls(row['grossmargin'])
        int_exp = clean_nulls(row['intexp'])
        inv_cap = clean_nulls(row['invcap'])
        inv_cap_avg = clean_nulls(row['invcapavg'])
        inventory = clean_nulls(row['inventory'])
        investments = clean_nulls(row['investments'])
        investments_c = clean_nulls(row['investmentsc'])
        investments_nc = clean_nulls(row['investmentsnc'])
        liabilities = clean_nulls(row['liabilities'])
        liabilities_c = clean_nulls(row['liabilitiesc'])
        liabilities_nc = clean_nulls(row['liabilitiesnc'])
        market_cap = clean_nulls(row['marketcap'])
        ncf = clean_nulls(row['ncf'])
        ncf_bus = clean_nulls(row['ncfbus'])
        ncf_common = clean_nulls(row['ncfcommon'])
        ncf_debt = clean_nulls(row['ncfdebt'])
        ncf_div = clean_nulls(row['ncfdiv'])
        ncf_f = clean_nulls(row['ncff'])
        ncf_i = clean_nulls(row['ncfi'])
        ncf_inv = clean_nulls(row['ncfinv'])
        ncf_o = clean_nulls(row['ncfo'])
        ncf_x = clean_nulls(row['ncfx'])
        net_inc = clean_nulls(row['netinc'])
        net_inc_cmn = clean_nulls(row['netinccmn'])
        net_inc_cmn_usd = clean_nulls(row['netinccmnusd'])
        net_inc_dis = clean_nulls(row['netincdis'])
        net_inc_nci = clean_nulls(row['netincnci'])
        net_margin = clean_nulls(row['netmargin'])
        op_ex = clean_nulls(row['opex'])
        op_inc = clean_nulls(row['opinc'])
        payables = clean_nulls(row['payables'])
        payout_ratio = clean_nulls(row['payoutratio'])
        pb = clean_nulls(row['pb'])
        pe = clean_nulls(row['pe'])
        pe1 = clean_nulls(row['pe1'])
        ppne_net = clean_nulls(row['ppnenet'])
        pref_div_is = clean_nulls(row['prefdivis'])
        price = clean_nulls(row['price'])
        ps = clean_nulls(row['ps'])
        ps1 = clean_nulls(row['ps1'])
        receivables = clean_nulls(row['receivables'])
        ret_earn = clean_nulls(row['retearn'])
        revenue = clean_nulls(row['revenue'])
        revenue_usd = clean_nulls(row['revenueusd'])
        rnd = clean_nulls(row['rnd'])
        roa = clean_nulls(row['roa'])
        roe = clean_nulls(row['roe'])
        roic = clean_nulls(row['roic'])
        ros = clean_nulls(row['ros'])
        sb_comp = clean_nulls(row['sbcomp'])
        sgna = clean_nulls(row['sgna'])
        share_factor = clean_nulls(row['sharefactor'])
        shares_bas = clean_nulls(row['sharesbas'])
        shares_wa = clean_nulls(row['shareswa'])
        shares_wa_dil = clean_nulls(row['shareswadil'])
        sps = clean_nulls(row['sps'])
        tangibles = clean_nulls(row['tangibles'])
        tax_assets = clean_nulls(row['taxassets'])
        tax_exp = clean_nulls(row['taxexp'])
        tax_liabilities = clean_nulls(row['taxliabilities'])
        tbvps = clean_nulls(row['tbvps'])
        working_capital = clean_nulls(row['workingcapital'])

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
        self.session.add(fundamentals)
