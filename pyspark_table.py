class UPriceDepoLogStatistics(ML360MainClassWithSparkPy):


    def filter_private_deal(
        self,
        df,
        col_deal_dt='deal_dt',
        col_ccy='ccy_cd',
        col_amount='deal_amt',
        col_term='deal_term_cnt'

    ):
        """
        Функция-фильтр оставляет только котировки с непубличными условиями
        """
        df = df\
        .filter(F.col(col_ccy).isin('RUB', 'RUR'))\
        .filter(
            (F.col(col_amount) >= 10_000_000)
                        & (
                     (F.col(col_deal_dt) >= '2023-04-01') & (
                            ((F.col(col_term) < 7))
                            |
                            ((F.col(col_term) >= 7) & (F.col(col_amount) >= 30_000_000))
                     )
                    |
                    (F.col(col_deal_dt) < '2023-04-01') & (
                            (F.col(col_term) < 7)
                            |
                            (F.col(col_term) >= 7) & (F.col(col_amount) >= 100_000_000))
                    )
                )
        return df

    def get_org_head_data(self):
        """
        """
        ucp_org_map_id = self.sqlContext.read.table('v_p4d_r_epk_org_rep_id')\
            .withColumn(
                'rn',
                F.row_number().over(Window.partitionBy(F.col('report_id')).orderBy(F.desc('rep_date')))
            ).filter(F.col('rn') == 1).select(
                F.col('report_id'),
                F.col('crm_id')
            )

        r_org0 = self.sqlContext.read.table('v_p4d_ucp_r_organization')\
            .filter(
            F.col('inn').isNotNull()
            & (F.col('clientcategory_name') == 'Головная организация')
            & F.col('report_id').isNotNull()
        )\
        .withColumn('rn_inn', F.row_number().over(Window.partitionBy(F.col('inn')).orderBy(F.desc('report_id'))))

        holding_struct = self.sqlContext.read.table('v_p4d_r_holding_ucpkb_struct')

        r_org = r_org0.alias('r_org0').filter(F.col('rn_inn') == 1)\
            .join(ucp_org_map_id.alias('ucp_org_map_id'), on=(F.col('r_org0.report_id') == F.col('ucp_org_map_id.report_id')), how='left')\
            .join(holding_struct.alias('holding_struct'), on=(F.col('r_org0.id') == F.col('holding_struct.fromparty_id_ucp')), how='left')\
            .withColumn('is_in_holding', F.when(F.col('holding_struct.holding_id_head').isNotNull(), 1).otherwise(0))\
        .select(
            F.col('inn').alias('inn_num'),
            'id',
            'crm_id',
            F.col('crm_id').alias('crm_id_head'),
            F.col('kpp'),
            F.col('ogrn'),
            F.col('okpo'),
            F.col('okogu'),
            F.col('okatocode'),
            F.col('okfscode'),
            F.col('okvedcode_main'),
            F.col('name'),
            F.col('name_short'),
            F.col('liquidation_statustype'),
            F.col('liquidation_statustype_name'),
            F.col('countryresident_code'),
            F.col('countryresident_name'),
            F.col('qualifier_code'),
            F.col('crm_qualifier_code'),
            F.col('crm_qualifier_name'),
            F.col('macroindustry_code'),
            F.col('legalclienttype'),
            F.col('legalclienttype_name'),
            F.col('crm_macroindustry_name'),
            F.col('macroindustry_sector_code'),
            F.col('crm_macroindustry_sector_name'),
            F.col('crm_industry_name'),
            F.col('crm_refdepartment'),
            F.col('crm_refdepartment_name'),
            F.col('crm_keyclientflag'),
            F.col('legalclassification_name'),
            F.col('legalclassification_shortname'),
            F.col('subsegment'),
            F.col('crm_segment'),
            F.col('r_org0.report_id'),
            F.col('cooperation'),
            F.col('tb_id'),
            F.col('tb_name'),
            F.col('clienttype_oud'),
            F.col('priority_code'),
            F.col('priority_description'),
            F.col('priority_name'),
            F.col('gosb_id'),
            F.col('gosb_name'),
            F.col('gosb_short_business_code'),
            F.col('status').alias('crm_status'),
            F.col('kio'),
            F.col('holding_id_head'),
            F.col('is_in_holding'),
            F.col('statustype_for_status_name'),
            F.col('statustype_for_status_code'),
            F.col('kindofactivity_name'),
            F.col('kindofactivity_number'),
            F.col('oktmocode'),
            F.col('privatebanking'),
            F.col('top_fot'),
            F.col('budget_recipient'),
            F.col('r_org0.src_ctl_validfrom'),
            F.col('fromparty_id_ucp'),
            F.col('fromparty_name')
            )

        return r_org

    def markup_quotes_deals_rejects(self, df, sdf_status_model_asfk):
        """
        """
        @F.udf(t.IntegerType())
        def f_success(
            calc_is_accepted,
            calc_is_annuled,
            rn_id_calc,
            rn_condition,
            is_annuled,
            greedy_condition_status_is_accepted,
            is_offset
        ):
            def f_nvl(x):
                if x is None:
                    return 0
                return x
            if all([
                calc_is_accepted == 1,
                f_nvl(calc_is_annuled) == 0,
                rn_id_calc == 1
            ]):
                return 1
            elif all([
                rn_condition == 1,
                f_nvl(is_annuled) == 0,
                f_nvl(greedy_condition_status_is_accepted) == 0,
                f_nvl(is_offset) == 0
            ]):
                return 0
            return None

        sdfl_private_tmp = df.join(sdf_status_model_asfk, how='left', on=['customer_nm', 'status_cd'])

        sdfl_private_marked = sdfl_private_tmp\
            .withColumn('rn_id_calc', F.row_number().over(Window.partitionBy('pass_through_calc_id').orderBy(F.desc('calculation_dttz'))))\
            .withColumn('rn_condition', F.row_number().over(Window.partitionBy(
                'pass_through_calc_id',
                'inn_num',
                'product_cd',
                'value_dt',
                'deal_term_cnt',
                'deal_amt',
                'rec_rate'
            ).orderBy(F.desc('calculation_dttz'))))\
            .withColumn('calc_is_accepted', F.max('is_accepted').over(Window.partitionBy('pass_through_calc_id')))\
            .withColumn('calc_is_annuled', F.max('is_annuled').over(Window.partitionBy('pass_through_calc_id')))\
            .withColumn('greedy_condition_status_is_accepted', F.max('is_accepted').over(Window.partitionBy(
                'pass_through_calc_id',
                'inn_num',
                'product_cd',
                'value_dt',
                'deal_term_cnt',
                'deal_amt',
                'rec_rate'
            ))).withColumn(
                'success',
            f_success(
                    'calc_is_accepted',
                    'calc_is_annuled',
                    'rn_id_calc',
                    'rn_condition',
                    'is_annuled',
                    'greedy_condition_status_is_accepted',
                    'is_offset'
                )).filter(F.col('success').isNotNull()).drop(
                    'rn_id_calc',
                    'rn_condition',
                    'is_accepted',
                    'is_annuled',
                    'is_offset',
                    'calc_is_accepted',
                    'calc_is_annuled',
                    'greedy_condition_status_is_accepted'
                )

        return sdfl_private_marked

    def get_status_model_asfk(self):
        """
        Функция возвращает таблицу описывающая статусную модель для разметки котировок по депозитам на согласия и отказы
        """
        df = self.sqlContext.createDataFrame(self.sc.parallelize(
                 [
                     t.Row(customer_nm='urn:sbrfsystems:99-crmorg',status_cd='SendClient',is_accepted=1,is_annuled=0,is_offset=0),
                     t.Row(customer_nm='urn:sbrfsystems:99-dogma',status_cd='TradeDone',is_accepted=1,is_annuled=0,is_offset=0),
                     t.Row(customer_nm='urn:sbrfsystems:99-efx',status_cd='TradeDone',is_accepted=1,is_annuled=0,is_offset=0),
                     t.Row(customer_nm='urn:sbrfsystems:99-pprb',status_cd='BreachDealClosed',is_accepted=1,is_annuled=0,is_offset=1),
                     t.Row(customer_nm='urn:sbrfsystems:99-pprb',status_cd='Closed',is_accepted=1,is_annuled=0,is_offset=1),
                     t.Row(customer_nm='urn:sbrfsystems:99-pprb',status_cd='DealDone',is_accepted=1,is_annuled=0,is_offset=0),
                     t.Row(customer_nm='urn:sbrfsystems:99-pprb',status_cd='EarlyTerminated',is_accepted=1,is_annuled=0,is_offset=1),
                     t.Row(customer_nm='urn:sbrfsystems:99-pprb',status_cd='MovedToEKS',is_accepted=1,is_annuled=0,is_offset=0),
                     t.Row(customer_nm='urn:sbrfsystems:99-pprb',status_cd='Annulled',is_accepted=0,is_annuled=1,is_offset=0),
                 ]
             ))
        return df



    def get_depo_logs(self, start_dt, end_dt, src_table_cons_calculation_fct, col_deal_dt):
        """
        Функция получения таблицы ....
        """
        lag_start_dt = datetime.strftime(datetime.strptime(str(start_dt),'%Y-%m-%d') - timedelta(30),'%Y-%m-%d')
        df = self.sqlContext.table(src_table_cons_calculation_fct)\
            .filter(F.col(col_deal_dt).between(lag_start_dt, end_dt))

        df = self.filter_private_deal(df)
        sdf_status_model_asfk = self.get_status_model_asfk()
        sdfl_private_marked = self.markup_quotes_deals_rejects(df, sdf_status_model_asfk)
        r_org = self.get_org_head_data()
        df_logs = sdfl_private_marked.join(
            r_org,
            on='inn_num',
            how='left'
        ).select(
            F.col('inn_num'),
            F.col('customer_nm'),
            F.col('status_cd'),
            F.col('ccy_cd'),
            F.col('deal_dt'),
            F.col('interest_rate'),
            F.col('model_nm'), #empty
            F.col('mosprime_rate'),
            F.col('fund_rate'),
            F.col('interest_payment_basis_cd'),
            F.col('model_ord'), #null only
            F.col('author_id'),
            F.col('business_block_cd'),
            F.col('calculation_dttz'),
            F.col('client_nm'),
            F.col('deal_term_cnt'),
            F.col('end_dt'),
            F.col('process_run_id'),
            F.col('calc_id'),
            F.col('client_epk_id'),
            F.col('has_top_up_option_flg'),
            F.col('has_withdrawal_option_flg'),
            F.col('limit_rate'),
            F.col('load_dttz'),
            F.col('max_rate'),
            F.col('model_response_status_cd'), #empty
            F.col('pass_through_calc_id'),
            F.col('product_cd'),
            F.col('public_rate'),
            F.col('value_dt'),
            F.col('author_nm'),
            F.col('client_crm_id'),
            F.col('deal_amt'),
            F.col('error_dsc'),
            F.col('ets_rate'),
            F.col('is_model_rate_flg'),
            F.col('model_rate'),
            F.col('operation_id'),
            F.col('prev_calc_id'),
            F.col('rec_rate'),
            F.col('tb_cd'),
            F.col('compensation_mark_flg'),
            F.col('eva_rate'),
            F.col('sens_flg'),
            F.col('delta_limit_amt'),
            F.col('msp_flg'),
            F.col('indicative_eva_rate'),
            F.col('resident_flg'),
            F.col('kio_cd'),
            F.col('option_price_rate'),
            F.col('target_eva_rate'),
            F.col('single_deal_amt'),
            F.col('success'),
            F.col('id'), #EPK
            F.col('crm_id_head'), #EPK
            F.col('kpp'), #EPK
            F.col('ogrn'), #EPK
            F.col('okpo'), #EPK
            F.col('okogu'), #EPK
            F.col('okatocode'), #EPK
            F.col('okfscode'), #EPK
            F.col('okvedcode_main'), #EPK
            F.col('name'), #EPK
            F.col('name_short'), #EPK
            F.col('liquidation_statustype'), #EPK
            F.col('liquidation_statustype_name'), #EPK
            F.col('countryresident_code'), #EPK
            F.col('countryresident_name'), #EPK
            F.col('qualifier_code'), #EPK
            F.col('crm_qualifier_code'), #EPK
            F.col('crm_qualifier_name'), #EPK
            F.col('macroindustry_code'), #EPK
            F.col('legalclienttype'), #EPK
            F.col('legalclienttype_name'), #EPK
            F.col('crm_macroindustry_name'), #EPK
            F.col('macroindustry_sector_code'), #EPK
            F.col('crm_macroindustry_sector_name'), #EPK
            F.col('crm_industry_name'), #EPK
            F.col('crm_refdepartment'), #EPK
            F.col('crm_refdepartment_name'), #EPK
            F.col('crm_keyclientflag'), #EPK
            F.col('legalclassification_name'), #EPK
            F.col('legalclassification_shortname'), #EPK
            F.col('subsegment'), #EPK
            F.col('crm_segment'), #EPK
            F.col('report_id'), #EPK
            F.col('cooperation'), #EPK
            F.col('tb_id'), #EPK
            F.col('tb_name'), #EPK
            F.col('clienttype_oud'), #EPK
            F.col('priority_code'), #EPK
            F.col('priority_description'), #EPK
            F.col('priority_name'), #EPK
            F.col('gosb_id'), #EPK
            F.col('gosb_name'), #EPK
            F.col('gosb_short_business_code'), #EPK
            F.col('crm_status'), #EPK
            F.col('kio'), #EPK
            F.col('holding_id_head'), #EPK
            F.col('is_in_holding'), #EPK
            F.col('statustype_for_status_name'), #EPK
            F.col('statustype_for_status_code'), #EPK
            F.col('kindofactivity_name'), #EPK
            F.col('kindofactivity_number'), #EPK
            F.col('oktmocode'), #EPK
            F.col('privatebanking'), #EPK
            F.col('top_fot'), #EPK
            F.col('budget_recipient'), #EPK
            F.col('src_ctl_validfrom'), #EPK
            F.col('fromparty_id_ucp'), #EPK
            F.col('fromparty_name') #EPK
        )
        return df_logs


    def calculation_statistics(self, df_src):
        """
        Функция расчета статистик по похожим сделкам и отказам для кажой котировки за 7 и 30 дней скользящим окном
        """

        @F.udf(t.IntegerType())
        def f_ETS_INTERVAL(x):
            ETS_INTERVAL = [0, 3, 7, 14, 21, 31, 61, 92, 122, 153, 183, 274, 366, 549, 731, 1096, 1827, 2557, 3653, 5479]
            return sum(map(lambda i: i < x,ETS_INTERVAL)) - 1

        @F.udf(t.IntegerType())
        def f_AMOUNT_INTERVAL(x):
            AMOUNT_INTERVAL = [0, 10000000, 30000000, 50000000, 100000000, 250000000, 500000000, 2000000000]
            return sum(map(lambda i: i < x,AMOUNT_INTERVAL)) - 1

        @F.udf(t.IntegerType())
        def target_revers(x):
            return [1,0][x]


        df = df_src\
        .withColumn('days_range_ets',f_ETS_INTERVAL('deal_term_cnt'))\
        .withColumn('summ_range_dot',f_AMOUNT_INTERVAL('deal_amt'))\
        .withColumn('margin',F.col('ets_rate') - F.col('interest_rate'))\
        .select(
            F.col('deal_dt'), #date_calc
            F.col('ets_rate'),
            F.col('limit_rate'),
            F.col('margin'),
            F.col('max_rate'),
            F.col('success'),
            F.col('product_cd'),
            F.col('days_range_ets'),
            F.col('summ_range_dot'),
            F.col('crm_id_head')
        )


        df_ = df\
            .withColumn('ets_rate',F.lit(None).cast(t.DecimalType(38,8)))\
            .withColumn('limit_rate',F.lit(None).cast(t.DecimalType(38,8)))\
            .withColumn('margin', F.lit(None).cast(t.DecimalType(38,7)))\
            .withColumn('success',target_revers('success'))

        df_dev = df.select(df_.columns)\
            .union(df_)\
            .orderBy('deal_dt')



        def count_them_all(
            data,
            li = ['ets_rate','limit_rate','margin', 'max_rate'],
            part = ['success','product_cd','days_range_ets','summ_range_dot','crm_id_head']
        ):
            def return_cols(rang_down, columns):
                w = Window.partitionBy(part)\
                 .orderBy(
                    F.round((F.col('deal_dt').cast(t.TimestampType()).cast(t.LongType()) / F.lit(86400)))
            )\
                 .rangeBetween(-rang_down, -1)
                 # -1 что б не цеплять текущий период
                cols_with_window = list(map(lambda x: F.sum(x).over(w).alias(f'{x}_sum_d{rang_down}'),columns)) +\
                    list(map(lambda x: F.avg(x).over(w).alias(f'{x}_mean_d{rang_down}'),columns)) +\
                    list(map(lambda x: F.stddev(x).over(w).alias(f'{x}_stddev_d{rang_down}'),columns)) +\
                    list(map(lambda x: F.min(x).over(w).alias(f'{x}_min_d{rang_down}'),li)) +\
                    list(map(lambda x: F.max(x).over(w).alias(f'{x}_max_d{rang_down}'),li)) +\
                    list(map(lambda x: F.skewness(x).over(w).alias(f'{x}_skewness_d{rang_down}'),columns)) +\
                    list(map(lambda x: F.percentile_approx(x, 0.5).over(w).alias(f'{x}_median_d{rang_down}'),columns)) +\
                    list(map(lambda x: F.kurtosis(x).over(w).alias(f'{x}_kurt_d{rang_down}'),columns))

                return cols_with_window

            return data.select(
                data.columns +
                return_cols(7,li) +
                return_cols(30,li)
            ).drop(*li).distinct()

        res = count_them_all(
            data=df_dev,
        )



        def rename_cols(
            data,
            prefix,
            sufux='',
            exclude_cols = ['product_cd','days_range_ets','summ_range_dot','crm_id_head', 'deal_dt']
        ):
            return data.select(exclude_cols + [F.col(col).alias(prefix + col + sufux) for col in data.columns if col not in exclude_cols])

        res_1 = rename_cols(res.filter(F.col('success') == 1).drop('success'), 'roll_')
        res_0 = rename_cols(res.filter(F.col('success') == 0).drop('success'), 'roll_reject_')

        li = ['ets_rate','limit_rate','margin', 'max_rate']

        res_1 = res_1.withColumn('roll_fuzzy_deals_count_d7', (F.col('roll_'+li[0]+'_sum_d7')/F.col('roll_'+li[0]+'_mean_d7')).cast(t.IntegerType()))\
            .withColumn('roll_fuzzy_deals_count_d30', (F.col('roll_'+li[0]+'_sum_d30')/F.col('roll_'+li[0]+'_mean_d30')).cast(t.IntegerType()))
        res_0 = res_0.withColumn('roll_fuzzy_reject_count_d7', (F.col('roll_reject_'+li[0]+'_sum_d7')/F.col('roll_reject_'+li[0]+'_mean_d7')).cast(t.IntegerType()))\
            .withColumn('roll_fuzzy_reject_count_d30', (F.col('roll_reject_'+li[0]+'_sum_d30')/F.col('roll_reject_'+li[0]+'_mean_d30')).cast(t.IntegerType()))


        join_cols=['deal_dt', 'product_cd','days_range_ets','summ_range_dot','crm_id_head']
        res_join = res_1.join(res_0, on=join_cols,how='inner')

        final_data = df_src\
        .withColumn('days_range_ets', f_ETS_INTERVAL('deal_term_cnt'))\
        .withColumn('summ_range_dot', f_AMOUNT_INTERVAL('deal_amt'))\
        .withColumn('margin', F.col('ets_rate') - F.col('interest_rate'))\
        .withColumn('mon', F.last_day('deal_dt').cast(t.StringType()))\
        .join(
            res_join,
            on=join_cols,
            how='left'
        )
        return final_data

    #################################################################################
    def run(self, spark, config, start_dt, end_dt):
        self.logger.info("Старт расчёта UPriceDepoLogStatistics")
        sqlContext = HiveContext(self.sc)

        self.logger.info("Starting function get_depo_logs()")
        df_src = self.get_depo_logs(
            start_dt=start_dt,
            end_dt=end_dt,
            src_table_cons_calculation_fct='v_lb_cons_calculation_fct',
            col_deal_dt='deal_dt'

        )
        self.logger.info("Starting function calculation_statistics()")

        df = self.calculation_statistics(df_src)\
            .filter(F.col('deal_dt').between(start_dt, end_dt))



        self.jvm.LoadFeatureTable.writeFeatureTable(df._jdf, ['mon'], spark, config)

def main():
    dm = UPriceDepoLogStatistics(
        app_name='u_price_depo_log_statistics',
        init_start_date='2021-01-01',
        module_name='u_price_depo_log_statistics'
    )
    dm.start()


if __name__ == "__main__":
    main()