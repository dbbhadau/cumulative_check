include: "//origin_attribution/Views/refined_campaign_cohort_device_activity.view.lkml"
# include: "//origin_attribution/Admon_Refinement/Views/origin_device_activity_admon.view.lkml"

view: origin_campaign_cohort_device_activity_admon { #remove refined in view name to avoid confusion on existing Looker assets
  derived_table: {
    datagroup_trigger: ua_refresh
    create_process: {
      sql_step: CREATE TABLE #players DISTKEY(attribution_cohort_key) AS
                SELECT
                    devices.origin_id
                  , devices.attribution_cohort_key
                  , DATEDIFF(day, devices.install_dt, device_activity_admon.event_dt) AS event_age
                  , DATEDIFF(day, devices.install_dt, CURRENT_DATE) AS cohort_age
                  , SUM(device_activity_admon.ad_revenue) AS ad_revenue
                  , SUM(device_activity_admon.offerwall_revenue) AS offerwall_revenue
                  , SUM(device_activity_admon.ad_impressions) AS ad_impressions --***
                FROM ${origin_devices.SQL_TABLE_NAME} devices
                LEFT JOIN ${origin_device_activity_admon.SQL_TABLE_NAME} AS device_activity_admon --***
                  USING (origin_id, game_name)
                WHERE
                  COALESCE(ad_revenue,0) > 0
                AND
                  DATEDIFF(day, devices.install_dt, device_activity_admon.event_dt) <= 360
                GROUP BY 1,2,3,4
                ;;

        sql_step: CREATE TABLE #full_aggregate DISTKEY(attribution_cohort_key) AS
                SELECT
                    attribution_cohort_key
                  , COUNT(DISTINCT devices.origin_id) AS ad_engaged_users --***
                  , SUM(ad_revenue) AS ad_revenue
                  , SUM(device_activity_admon.offerwall_revenue) AS offerwall_revenue
                  , SUM(ad_impressions) AS ad_impressions --***
                FROM ${origin_devices.SQL_TABLE_NAME} AS devices
                LEFT JOIN ${origin_device_activity_admon.SQL_TABLE_NAME} AS device_activity_admon --***
                  ON (devices.origin_id = device_activity_admon.origin_id)
                WHERE COALESCE(ad_revenue,0) > 0
                GROUP BY 1
                ;;

          sql_step: DROP TABLE IF EXISTS ${SQL_TABLE_NAME} ;;

          sql_step: CREATE TABLE ${SQL_TABLE_NAME} DISTKEY(attribution_cohort_key) AS

                            SELECT
                              attribution_cohort_key
                              , DATEDIFF(day, devices.install_dt, CURRENT_DATE) cohort_age_supplemental
                              , NVL(cohort_age, cohort_age_supplemental) cohort_age
                              , COALESCE(full_aggregate.ad_engaged_users,0) AS ad_engaged_users --***
                              , COALESCE(full_aggregate.ad_revenue,0) AS ad_revenue --***
                              , COALESCE(full_aggregate.offerwall_revenue,0) AS offerwall_revenue
                              , COALESCE(full_aggregate.ad_impressions,0) AS ad_impressions --***
                              , COUNT(DISTINCT CASE WHEN event_age <= 0 AND cohort_age >= 0 THEN players.origin_id ELSE NULL END) AS d0_ad_engaged_users --***
                              , COUNT(DISTINCT CASE WHEN event_age <= 1 AND cohort_age >= 1 THEN players.origin_id ELSE NULL END) AS d1_ad_engaged_users --***
                              , COUNT(DISTINCT CASE WHEN event_age <= 3 AND cohort_age >= 3 THEN players.origin_id ELSE NULL END) AS d3_ad_engaged_users --***
                              , COUNT(DISTINCT CASE WHEN event_age <= 7 AND cohort_age >= 7 THEN players.origin_id ELSE NULL END) AS d7_ad_engaged_users --***
                              , COUNT(DISTINCT CASE WHEN event_age <= 14 AND cohort_age >= 14 THEN players.origin_id ELSE NULL END) AS d14_ad_engaged_users --***
                              , COUNT(DISTINCT CASE WHEN event_age <= 21 AND cohort_age >= 21 THEN players.origin_id ELSE NULL END) AS d21_ad_engaged_users --***
                              , COUNT(DISTINCT CASE WHEN event_age <= 30 AND cohort_age >= 30 THEN players.origin_id ELSE NULL END) AS d30_ad_engaged_users --***
                              , COUNT(DISTINCT CASE WHEN event_age <= 60 AND cohort_age >= 60 THEN players.origin_id ELSE NULL END) AS d60_ad_engaged_users --***
                              , COUNT(DISTINCT CASE WHEN event_age <= 90 AND cohort_age >= 90 THEN players.origin_id ELSE NULL END) AS d90_ad_engaged_users --***
                              , SUM(CASE WHEN event_age <= 0 AND cohort_age >= 0 THEN players.ad_revenue ELSE NULL END) AS d0_ad_revenue
                              , SUM(CASE WHEN event_age <= 1 AND cohort_age >= 1 THEN players.ad_revenue ELSE NULL END) AS d1_ad_revenue
                              , SUM(CASE WHEN event_age <= 3 AND cohort_age >= 3 THEN players.ad_revenue ELSE NULL END) AS d3_ad_revenue
                              , SUM(CASE WHEN event_age <= 7 AND cohort_age >= 7 THEN players.ad_revenue ELSE NULL END) AS d7_ad_revenue
                              , SUM(CASE WHEN event_age <= 14 AND cohort_age >= 14 THEN players.ad_revenue ELSE NULL END) AS d14_ad_revenue
                              , SUM(CASE WHEN event_age <= 21 AND cohort_age >= 21 THEN players.ad_revenue ELSE NULL END) AS d21_ad_revenue
                              , SUM(CASE WHEN event_age <= 15 AND cohort_age >= 15 THEN players.ad_revenue ELSE NULL END) AS d15_ad_revenue
                              , SUM(CASE WHEN event_age <= 30 AND cohort_age >= 30 THEN players.ad_revenue ELSE NULL END) AS d30_ad_revenue
                              , SUM(CASE WHEN event_age <= 60 AND cohort_age >= 60 THEN players.ad_revenue ELSE NULL END) AS d60_ad_revenue
                              , SUM(CASE WHEN event_age <= 90 AND cohort_age >= 90 THEN players.ad_revenue ELSE NULL END) AS d90_ad_revenue
                              , SUM(CASE WHEN event_age <= 120 AND cohort_age >= 120 THEN players.ad_revenue ELSE NULL END) AS d120_ad_revenue
                              , SUM(CASE WHEN event_age <= 150 AND cohort_age >= 150 THEN players.ad_revenue ELSE NULL END) AS d150_ad_revenue
                              , SUM(CASE WHEN event_age <= 180 AND cohort_age >= 180 THEN players.ad_revenue ELSE NULL END) AS d180_ad_revenue
                              , SUM(CASE WHEN event_age <= 210 AND cohort_age >= 210 THEN players.ad_revenue ELSE NULL END) AS d210_ad_revenue
                              , SUM(CASE WHEN event_age <= 240 AND cohort_age >= 240 THEN players.ad_revenue ELSE NULL END) AS d240_ad_revenue
                              , SUM(CASE WHEN event_age <= 270 AND cohort_age >= 270 THEN players.ad_revenue ELSE NULL END) AS d270_ad_revenue
                              , SUM(CASE WHEN event_age <= 300 AND cohort_age >= 300 THEN players.ad_revenue ELSE NULL END) AS d300_ad_revenue
                              , SUM(CASE WHEN event_age <= 330 AND cohort_age >= 330 THEN players.ad_revenue ELSE NULL END) AS d330_ad_revenue
                              , SUM(CASE WHEN event_age <= 360 AND cohort_age >= 360 THEN players.ad_revenue ELSE NULL END) AS d360_ad_revenue
                              , SUM(CASE WHEN event_age <= 0 AND cohort_age >= 0 THEN players.ad_impressions ELSE NULL END) AS d0_ad_impressions --***
                              , SUM(CASE WHEN event_age <= 1 AND cohort_age >= 1 THEN players.ad_impressions ELSE NULL END) AS d1_ad_impressions --***
                              , SUM(CASE WHEN event_age <= 3 AND cohort_age >= 3 THEN players.ad_impressions ELSE NULL END) AS d3_ad_impressions --***
                              , SUM(CASE WHEN event_age <= 7 AND cohort_age >= 7 THEN players.ad_impressions ELSE NULL END) AS d7_ad_impressions --***
                              , SUM(CASE WHEN event_age <= 14 AND cohort_age >= 14 THEN players.ad_impressions ELSE NULL END) AS d14_ad_impressions --***
                              , SUM(CASE WHEN event_age <= 21 AND cohort_age >= 21 THEN players.ad_impressions ELSE NULL END) AS d21_ad_impressions --***
                              , SUM(CASE WHEN event_age <= 30 AND cohort_age >= 30 THEN players.ad_impressions ELSE NULL END) AS d30_ad_impressions --***
                              , SUM(CASE WHEN event_age <= 60 AND cohort_age >= 60 THEN players.ad_impressions ELSE NULL END) AS d60_ad_impressions --***
                              , SUM(CASE WHEN event_age <= 90 AND cohort_age >= 90 THEN players.ad_impressions ELSE NULL END) AS d90_ad_impressions --***
                              , SUM(CASE WHEN event_age <= 0 AND cohort_age >= 0 THEN players.offerwall_revenue ELSE NULL END) AS d0_ad_revenue_offerwall
                              , SUM(CASE WHEN event_age <= 1 AND cohort_age >= 1 THEN players.offerwall_revenue ELSE NULL END) AS d1_ad_revenue_offerwall
                              , SUM(CASE WHEN event_age <= 3 AND cohort_age >= 3 THEN players.offerwall_revenue ELSE NULL END) AS d3_ad_revenue_offerwall
                              , SUM(CASE WHEN event_age <= 7 AND cohort_age >= 7 THEN players.offerwall_revenue ELSE NULL END) AS d7_ad_revenue_offerwall
                              , SUM(CASE WHEN event_age <= 14 AND cohort_age >= 14 THEN players.offerwall_revenue ELSE NULL END) AS d14_ad_revenue_offerwall
                              , SUM(CASE WHEN event_age <= 21 AND cohort_age >= 21 THEN players.offerwall_revenue ELSE NULL END) AS d21_ad_revenue_offerwall
                              , SUM(CASE WHEN event_age <= 15 AND cohort_age >= 15 THEN players.offerwall_revenue ELSE NULL END) AS d15_ad_revenue_offerwall
                              , SUM(CASE WHEN event_age <= 30 AND cohort_age >= 30 THEN players.offerwall_revenue ELSE NULL END) AS d30_ad_revenue_offerwall
                              , SUM(CASE WHEN event_age <= 60 AND cohort_age >= 60 THEN players.offerwall_revenue ELSE NULL END) AS d60_ad_revenue_offerwall
                              , SUM(CASE WHEN event_age <= 90 AND cohort_age >= 90 THEN players.offerwall_revenue ELSE NULL END) AS d90_ad_revenue_offerwall
                              , SUM(CASE WHEN event_age <= 120 AND cohort_age >= 120 THEN players.offerwall_revenue ELSE NULL END) AS d120_ad_revenue_offerwall
                              , SUM(CASE WHEN event_age <= 150 AND cohort_age >= 150 THEN players.offerwall_revenue ELSE NULL END) AS d150_ad_revenue_offerwall
                              , SUM(CASE WHEN event_age <= 180 AND cohort_age >= 180 THEN players.offerwall_revenue ELSE NULL END) AS d180_ad_revenue_offerwall
                              , SUM(CASE WHEN event_age <= 210 AND cohort_age >= 210 THEN players.offerwall_revenue ELSE NULL END) AS d210_ad_revenue_offerwall
                              , SUM(CASE WHEN event_age <= 240 AND cohort_age >= 240 THEN players.offerwall_revenue ELSE NULL END) AS d240_ad_revenue_offerwall
                              , SUM(CASE WHEN event_age <= 270 AND cohort_age >= 270 THEN players.offerwall_revenue ELSE NULL END) AS d270_ad_revenue_offerwall
                              , SUM(CASE WHEN event_age <= 300 AND cohort_age >= 300 THEN players.offerwall_revenue ELSE NULL END) AS d300_ad_revenue_offerwall
                              , SUM(CASE WHEN event_age <= 330 AND cohort_age >= 330 THEN players.offerwall_revenue ELSE NULL END) AS d330_ad_revenue_offerwall
                              , SUM(CASE WHEN event_age <= 360 AND cohort_age >= 360 THEN players.offerwall_revenue ELSE NULL END) AS d360_ad_revenue_offerwall
                            FROM #full_aggregate full_aggregate
                            LEFT JOIN #players players
                              USING (attribution_cohort_key)
                            LEFT JOIN (SELECT DISTINCT attribution_cohort_key, install_dt FROM ${origin_devices.SQL_TABLE_NAME}) devices
                              USING (attribution_cohort_key)
                            GROUP BY 1,2,3,4,5,6,7
                          ;;
        }
      }

      dimension: attribution_cohort_key {
        type: string
        hidden: yes
        primary_key: yes
        sql: ${TABLE}.attribution_cohort_key ;;
      }

      dimension: cohort_age {
        description: "Days between cohort's install date and current date (src = MMP)"
        view_label: "Player Properties - Current"
        label: "Player Age - Current"
        hidden: yes
        type: number
        sql: ${TABLE}.cohort_age ;;
      }

      # Cohort Admon metrics
      measure: ad_engaged_users {
        type: sum
        view_label: "Cohort Revenue - Ad Mon"
        label: "Ad Engaged Users"
        description: "Count of ad engaged users (src = ironSource)."
        value_format_name: decimal_0
        sql: ${TABLE}.ad_engaged_users ;;
      }

      measure: ad_revenue {
        type: sum
        view_label: "Cohort Revenue - Ad Mon"
        label: "Ad Revenue"
        description: "Total net ad revenue in USD (src = ironSource). Ad mon revenue is already net when we receive it."
        value_format_name: usd_0
        sql: ${TABLE}.ad_revenue ;;
      }

      measure: ad_revenue_offerwall {
        type: sum
        view_label: "Cohort Revenue - Ad Mon"
        label: "Ad Revenue Offerwall"
        description: "Net Revenue in USD (src = tapjoy and ironSource) for ad_unit offerwall."
        value_format_name: usd_0
        sql: ${TABLE}.offerwall_revenue ;;
      }

      measure: ad_revenue_non_offerwall {
        type: number
        view_label: "Cohort Revenue - Ad Mon"
        label: "Ad Revenue Non-Offerwall"
        description: "Ad_unit non-offerwall revenue is mostly rewarded video revenue,interstitial revenue is nearly non-existent"
        value_format_name: usd_0
        sql: ${ad_revenue} - ${ad_revenue_offerwall};;
      }

      measure: ad_impressions {
        type: sum
        view_label: "Cohort Revenue - Ad Mon"
        label: "Ad Impressions"
        description: "Count of ad impressions (src = ironSource)"
        value_format_name: decimal_0
        sql: ${TABLE}.ad_impressions ;;
      }

      measure: ad_engaged_user_rate {
        type: number
        view_label: "Cohort Revenue - Ad Mon"
        label: "Ad Engaged User Rate"
        description: "Ad Engaged Users  (src = ironSource) / Installs (src = MMP)"
        value_format_name: percent_1
        sql: COALESCE(${ad_engaged_users}/NULLIF(${origin_campaign_cohort.cohort_size},0)::FLOAT,0) ;;
      }

      measure: avg_ad_revenue {
        type: number
        view_label: "Cohort Revenue - Ad Mon"
        label: "Average Ad Revenue"
        description: "Average gross revenue in USD (src = MMP). Gross revenue in USD is calculated using the price Tier - 0.01"
        value_format_name: usd
        sql: COALESCE(${ad_revenue}/NULLIF(${origin_campaign_cohort.cohort_size},0)::FLOAT,0) ;;
      }

      measure: avg_ad_revenue_offerwall {
        type: number
        view_label: "Cohort Revenue - Ad Mon"
        label: "Average Ad Revenue Offerwall"
        description: "Average gross revenue in USD (src = MMP) for ad unit offerwall it is calculated using the price Tier - 0.01"
        value_format_name: usd
        sql: COALESCE(${ad_revenue_offerwall}/NULLIF(${origin_campaign_cohort.cohort_size},0)::FLOAT,0) ;;
      }

      measure: avg_ad_revenue_non_offerwall {
        type: number
        view_label: "Cohort Revenue - Ad Mon"
        label: "Average Ad Revenue Non-Offerwall"
        description: "Average gross revenue in USD (src = MMP) for ad unit non-offerwall it is calculated using the price Tier - 0.01"
        value_format_name: usd
        sql: COALESCE((${ad_revenue_non_offerwall})/NULLIF(${origin_campaign_cohort.cohort_size},0)::FLOAT,0) ;;
      }

      measure: avg_ad_impressions {
        type: number
        view_label: "Cohort Revenue - Ad Mon"
        label: "Average Ad Impressions"
        description: "Average count of ad impressions (src = ironSource)."
        value_format_name: decimal_1
        sql: COALESCE(${ad_impressions}/NULLIF(${origin_campaign_cohort.cohort_size},0)::FLOAT,0) ;;
      }

      measure: ad_mon_arpu {
        type: number
        view_label: "Cohort Revenue - Ad Mon"
        label: "ARPU - Ad Mon"
        description: "Ad Revenue (src = ironSource) / Installs (src = MMP)"
        value_format_name: usd
        sql: COALESCE(${ad_revenue}/NULLIF(${origin_campaign_cohort.cohort_size},0)::FLOAT,0) ;;
      }

      measure: ad_mon_arpu_offerwall {
        type: number
        view_label: "Cohort Revenue - Ad Mon"
        label: "ARPU - Ad Mon Offerwall"
        description: "Ad Revenue Offerwall (src = tapjoy) / Installs (src = MMP)"
        value_format_name: usd
        sql: COALESCE(${ad_revenue_offerwall}/NULLIF(${origin_campaign_cohort.cohort_size},0)::FLOAT,0) ;;
      }

      measure: ad_mon_arpu_non_offerwall {
        type: number
        view_label: "Cohort Revenue - Ad Mon"
        label: "ARPU - Ad Mon Non-Offerwall"
        description: "Ad Revenue Non-Offerwall (src = ironSource) / Installs (src = MMP)"
        value_format_name: usd
        sql: COALESCE((${ad_revenue_non_offerwall}) /NULLIF(${origin_campaign_cohort.cohort_size},0)::FLOAT,0) ;;
      }

# Ad Engaged Users D[x] Rolling Sum
      measure: d0_ad_engaged_users {
        type: number
        group_label: "Ad Engaged Users"
        view_label: "Cumulative D[x] - Ad Mon"
        label: "D00 Ad Engaged Users"
        description: "Cumulative sum from life to D[x] (src = MMP)"
        value_format_name: decimal_0
        sql: COALESCE(SUM(${TABLE}.d0_ad_engaged_users),0) ;;
      }

      measure: d1_ad_engaged_users {
        type: number
        group_label: "Ad Engaged Users"
        view_label: "Cumulative D[x] - Ad Mon"
        label: "D01 Ad Engaged Users"
        description: "Cumulative sum from life to D[x] (src = MMP)"
        value_format_name: decimal_0
        sql: COALESCE(SUM(${TABLE}.d1_ad_engaged_users),0) ;;
      }

      measure: d3_ad_engaged_users {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Engaged Users"
        label: "D03 Ad Engaged Users"
        description: "Cumulative sum from life to D[x] (src = MMP)"
        value_format_name: decimal_0
        sql: COALESCE(SUM(${TABLE}.d3_ad_engaged_users),0) ;;
      }

      measure: d7_ad_engaged_users {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Engaged Users"
        label: "D07 Ad Engaged Users"
        description: "Cumulative sum from life to D[x] (src = MMP)"
        value_format_name: decimal_0
        sql: COALESCE(SUM(${TABLE}.d7_ad_engaged_users),0) ;;
      }

      measure: d14_ad_engaged_users {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Engaged Users"
        label: "D14 Ad Engaged Users"
        description: "Cumulative sum from life to D[x] (src = MMP)"
        value_format_name: decimal_0
        sql: COALESCE(SUM(${TABLE}.d14_ad_engaged_users),0) ;;
      }

      measure: d21_ad_engaged_users {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Engaged Users"
        label: "D21 Ad Engaged Users"
        description: "Cumulative sum from life to D[x] (src = MMP)"
        value_format_name: decimal_0
        sql: COALESCE(SUM(${TABLE}.d21_ad_engaged_users),0) ;;
      }

      measure: d30_ad_engaged_users {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Engaged Users"
        label: "D30 Ad Engaged Users"
        description: "Cumulative sum from life to D[x] (src = MMP)"
        value_format_name: decimal_0
        sql: COALESCE(SUM(${TABLE}.d30_ad_engaged_users),0) ;;
      }

      measure: d60_ad_engaged_users {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Engaged Users"
        label: "D60 Ad Engaged Users"
        description: "Cumulative sum from life to D[x] (src = MMP)"
        value_format_name: decimal_0
        sql: COALESCE(SUM(${TABLE}.d60_ad_engaged_users),0) ;;
      }

      measure: d90_ad_engaged_users {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Engaged Users"
        label: "D90 Ad Engaged Users"
        description: "Cumulative sum from life to D[x] (src = MMP)"
        value_format_name: decimal_0
        sql: COALESCE(SUM(${TABLE}.d90_ad_engaged_users),0) ;;
      }

# Engaged Rate D[x] (ad engaged user/cohort size) - instead of Conversion Rate D[x]
      measure: d0_ad_engaged {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Engaged Rate"
        label: "D00 Ad Engaged"
        description: "Cumulative transition to ad engaged user from life to D[x] (src = MMP)"
        value_format_name: percent_1
        sql: COALESCE((${d0_ad_engaged_users}/NULLIF(${campaign_cohort_device_activity.d0_cohort_size}::FLOAT,0)),0);;
      }

      measure: d1_ad_engaged {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Engaged Rate"
        label: "D01 Ad Engaged"
        description: "Cumulative transition to ad engaged user from life to D[x] (src = MMP)"
        value_format_name: percent_1
        sql: COALESCE((${d1_ad_engaged_users}/NULLIF(${campaign_cohort_device_activity.d1_cohort_size}::FLOAT,0)),0) ;;
      }

      measure: d3_ad_engaged {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Engaged Rate"
        label: "D03 Ad Engaged"
        description: "Cumulative transition to ad engaged user from life to D[x] (src = MMP)"
        value_format_name: percent_1
        sql: COALESCE((${d3_ad_engaged_users}/NULLIF(${campaign_cohort_device_activity.d3_cohort_size}::FLOAT,0)),0) ;;
      }

      measure: d7_ad_engaged {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Engaged Rate"
        label: "D07 Ad Engaged"
        description: "Cumulative transition to ad engaged user from life to D[x] (src = MMP)"
        value_format_name: percent_1
        sql: COALESCE((${d7_ad_engaged_users}/NULLIF(${campaign_cohort_device_activity.d7_cohort_size}::FLOAT,0)),0);;
      }

      measure: d14_ad_engaged {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Engaged Rate"
        label: "D14 Ad Engaged"
        description: "Cumulative transition to ad engaged user from life to D[x] (src = MMP)"
        value_format_name: percent_1
        sql: COALESCE((${d14_ad_engaged_users}/NULLIF(${campaign_cohort_device_activity.d14_cohort_size}::FLOAT,0)),0);;
      }

      measure: d21_ad_engaged {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Engaged Rate"
        label: "D21 Ad Engaged"
        description: "Cumulative transition to ad engaged user from life to D[x] (src = MMP)"
        value_format_name: percent_1
        sql: COALESCE((${d21_ad_engaged_users}/NULLIF(${campaign_cohort_device_activity.d21_cohort_size}::FLOAT,0)),0);;
      }

      measure: d30_ad_engaged {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Engaged Rate"
        label: "D30 Ad Engaged"
        description: "Cumulative transition to ad engaged user from life to D[x] (src = MMP)"
        value_format_name: percent_1
        sql: COALESCE((${d30_ad_engaged_users}/NULLIF(${campaign_cohort_device_activity.d30_cohort_size}::FLOAT,0)),0);;
      }

      measure: d60_ad_engaged {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Engaged Rate"
        label: "D60 Ad Engaged"
        description: "Cumulative transition to ad engaged user from life to D[x] (src = MMP)"
        value_format_name: percent_1
        sql: COALESCE((${d60_ad_engaged_users}/NULLIF(${campaign_cohort_device_activity.d60_cohort_size}::FLOAT,0)),0) ;;
      }

      measure: d90_ad_engaged {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Engaged Rate"
        label: "D90 Ad Engaged"
        description: "Cumulative transition to ad engaged user from life to D[x] (src = MMP)"
        value_format_name: percent_1
        sql: COALESCE((${d90_ad_engaged_users}/NULLIF(${campaign_cohort_device_activity.d90_cohort_size}::FLOAT,0)),0) ;;
      }

      # Ad Revenue D[x] Rolling Sum
      measure: d0_ad_revenue {
        type: number
        group_label: "Ad Revenue Proceeds"
        view_label: "Cumulative D[x] - Ad Mon"
        label: "D00 Ad Revenue"
        description: "Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d0_ad_revenue),0) ;;
      }

      measure: d1_ad_revenue {
        type: number
        group_label: "Ad Revenue Proceeds"
        view_label: "Cumulative D[x] - Ad Mon"
        label: "D01 Ad Revenue"
        description: "Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d1_ad_revenue),0) ;;
      }

      measure: d1_ad_revenue_unbaked {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds (Unbaked)"
        label: "D01 Ad Revenue (Unbaked)"
        description: "Unbaked - Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: SUM(CASE
                WHEN ${TABLE}.cohort_age >= 1 THEN ${TABLE}.d1_ad_revenue
                WHEN ${TABLE}.cohort_age < 1 THEN ${TABLE}.ad_revenue
                ELSE NULL
            END
            ) ;;
      }

      measure: d3_ad_revenue {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D03 Ad Revenue"
        description: "Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d3_ad_revenue),0) ;;
      }

      measure: d3_ad_revenue_unbaked {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds (Unbaked)"
        label: "D03 Ad Revenue (Unbaked)"
        description: "Unbaked - Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: SUM(CASE
                WHEN ${TABLE}.cohort_age >= 3 THEN ${TABLE}.d3_ad_revenue
                WHEN ${TABLE}.cohort_age < 3 THEN ${TABLE}.ad_revenue
                ELSE NULL
            END
            ) ;;
      }

      measure: d7_ad_revenue {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D07 Ad Revenue"
        description: "Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d7_ad_revenue),0) ;;
      }

      measure: d7_ad_revenue_unbaked {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds (Unbaked)"
        label: "D07 Ad Revenue (Unbaked)"
        description: "Unbaked - Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: SUM(CASE
                WHEN ${TABLE}.cohort_age >= 7 THEN ${TABLE}.d7_ad_revenue
                WHEN ${TABLE}.cohort_age < 7 THEN ${TABLE}.ad_revenue
                ELSE NULL
            END
            ) ;;
      }

      measure: d14_ad_revenue {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D14 Ad Revenue"
        description: "Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d14_ad_revenue),0) ;;
      }

      measure: d14_ad_revenue_unbaked {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds (Unbaked)"
        label: "D14 Ad Revenue (Unbaked)"
        description: "Unbaked - Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: SUM(CASE
                WHEN ${TABLE}.cohort_age >= 14 THEN ${TABLE}.d14_ad_revenue
                WHEN ${TABLE}.cohort_age < 14 THEN ${TABLE}.ad_revenue
                ELSE NULL
            END
            ) ;;
      }

      measure: d21_ad_revenue {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D21 Ad Revenue"
        description: "Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d21_ad_revenue),0) ;;
      }

      measure: d21_ad_revenue_unbaked {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds (Unbaked)"
        label: "D21 Ad Revenue (Unbaked)"
        description: "Unbaked - Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: SUM(CASE
                WHEN ${TABLE}.cohort_age >= 21 THEN ${TABLE}.d21_ad_revenue
                WHEN ${TABLE}.cohort_age < 21 THEN ${TABLE}.ad_revenue
                ELSE NULL
            END
            ) ;;
      }

      measure: d15_ad_revenue {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D15 Ad Revenue"
        description: "Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d15_ad_revenue),0) ;;
      }

      measure: d15_ad_revenue_unbaked {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds (Unbaked)"
        label: "D15 Ad Revenue (Unbaked)"
        description: "Unbaked - Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: SUM(CASE
                WHEN ${TABLE}.cohort_age >= 15 THEN ${TABLE}.d15_ad_revenue
                WHEN ${TABLE}.cohort_age < 15 THEN ${TABLE}.ad_revenue
                ELSE NULL
            END
            ) ;;
      }

      measure: d30_ad_revenue {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D30 Ad Revenue"
        description: "Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d30_ad_revenue),0) ;;
      }

      measure: d30_ad_revenue_unbaked {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds (Unbaked)"
        label: "D30 Ad Revenue (Unbaked)"
        description: "Unbaked - Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: SUM(CASE
                WHEN ${TABLE}.cohort_age >= 30 THEN ${TABLE}.d30_ad_revenue
                WHEN ${TABLE}.cohort_age < 30 THEN ${TABLE}.ad_revenue
                ELSE NULL
            END
            ) ;;
      }

      measure: d60_ad_revenue {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D60 Ad Revenue"
        description: "Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d60_ad_revenue),0) ;;
      }

      measure: d60_ad_revenue_unbaked {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds (Unbaked)"
        label: "D60 Ad Revenue (Unbaked)"
        description: "Unbaked - Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: SUM(CASE
                WHEN ${TABLE}.cohort_age >= 60 THEN ${TABLE}.d60_ad_revenue
                WHEN ${TABLE}.cohort_age < 60 THEN ${TABLE}.ad_revenue
                ELSE NULL
            END
            ) ;;
      }

      measure: d90_ad_revenue {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D90 Ad Revenue"
        description: "Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d90_ad_revenue),0) ;;
      }

      measure: d90_ad_revenue_unbaked {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds (Unbaked)"
        label: "D90 Ad Revenue (Unbaked)"
        description: "Unbaked - Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: SUM(CASE
                WHEN ${TABLE}.cohort_age >= 90 THEN ${TABLE}.d90_ad_revenue
                WHEN ${TABLE}.cohort_age < 90 THEN ${TABLE}.ad_revenue
                ELSE NULL
            END
            ) ;;
      }

      measure: d120_ad_revenue {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D120 Ad Revenue"
        description: "Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d120_ad_revenue),0) ;;
      }

      measure: d120_ad_revenue_unbaked {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds (Unbaked)"
        label: "D120 Ad Revenue (Unbaked)"
        description: "Unbaked - Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: SUM(CASE
                WHEN ${TABLE}.cohort_age >= 120 THEN ${TABLE}.d120_ad_revenue
                WHEN ${TABLE}.cohort_age < 120 THEN ${TABLE}.ad_revenue
                ELSE NULL
            END
            ) ;;
      }

      measure: d150_ad_revenue {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D150 Ad Revenue"
        description: "Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d150_ad_revenue),0) ;;
      }

      measure: d150_ad_revenue_unbaked {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds (Unbaked)"
        label: "D150 Ad Revenue (Unbaked)"
        description: "Unbaked - Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: SUM(CASE
                WHEN ${TABLE}.cohort_age >= 150 THEN ${TABLE}.d150_ad_revenue
                WHEN ${TABLE}.cohort_age < 150 THEN ${TABLE}.ad_revenue
                ELSE NULL
            END
            ) ;;
      }

      measure: d180_ad_revenue {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D180 Ad Revenue"
        description: "Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d180_ad_revenue),0) ;;
      }

      measure: d180_ad_revenue_unbaked {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds (Unbaked)"
        label: "D180 Ad Revenue (Unbaked)"
        description: "Unbaked - Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: SUM(CASE
                WHEN ${TABLE}.cohort_age >= 180 THEN ${TABLE}.d180_ad_revenue
                WHEN ${TABLE}.cohort_age < 180 THEN ${TABLE}.ad_revenue
                ELSE NULL
            END
            ) ;;
      }

      measure: d210_ad_revenue {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D210 Ad Revenue"
        description: "Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d210_ad_revenue),0) ;;
      }

      measure: d210_ad_revenue_unbaked {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds (Unbaked)"
        label: "D210 Ad Revenue (Unbaked)"
        description: "Unbaked - Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: SUM(CASE
                WHEN ${TABLE}.cohort_age >= 210 THEN ${TABLE}.d210_ad_revenue
                WHEN ${TABLE}.cohort_age < 210 THEN ${TABLE}.ad_revenue
                ELSE NULL
            END
            ) ;;
      }

      measure: d240_ad_revenue {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D240 Ad Revenue"
        description: "Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d240_ad_revenue),0) ;;
      }

      measure: d240_ad_revenue_unbaked {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds (Unbaked)"
        label: "D240 Ad Revenue (Unbaked)"
        description: "Unbaked - Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: SUM(CASE
                WHEN ${TABLE}.cohort_age >= 240 THEN ${TABLE}.d240_ad_revenue
                WHEN ${TABLE}.cohort_age < 240 THEN ${TABLE}.ad_revenue
                ELSE NULL
            END
            ) ;;
      }

      measure: d270_ad_revenue {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D270 Ad Revenue"
        description: "Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d270_ad_revenue),0) ;;
      }

      measure: d270_ad_revenue_unbaked {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds (Unbaked)"
        label: "D270 Ad Revenue (Unbaked)"
        description: "Unbaked - Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: SUM(CASE
                WHEN ${TABLE}.cohort_age >= 270 THEN ${TABLE}.d270_ad_revenue
                WHEN ${TABLE}.cohort_age < 270 THEN ${TABLE}.ad_revenue
                ELSE NULL
            END
            ) ;;
      }
      measure: d300_ad_revenue {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D300 Ad Revenue"
        description: "Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d300_ad_revenue),0) ;;
      }

      measure: d300_ad_revenue_unbaked {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds (Unbaked)"
        label: "D300 Ad Revenue (Unbaked)"
        description: "Unbaked - Cumulative sum from life to D[x] (src = ironSource)"
        value_format_name: usd_0
        sql: SUM(CASE
                WHEN ${TABLE}.cohort_age >= 300 THEN ${TABLE}.d300_ad_revenue
                WHEN ${TABLE}.cohort_age < 300 THEN ${TABLE}.ad_revenue
                ELSE NULL
            END
            ) ;;

        }
        measure: d330_ad_revenue {
          type: number
          view_label: "Cumulative D[x] - Ad Mon"
          group_label: "Ad Revenue Proceeds"
          label: "D330 Ad Revenue"
          description: "Cumulative sum from life to D[x] (src = ironSource)"
          value_format_name: usd_0
          sql: COALESCE(SUM(${TABLE}.d330_ad_revenue),0) ;;
        }

        measure: d330_ad_revenue_unbaked {
          type: number
          view_label: "Cumulative D[x] - Ad Mon"
          group_label: "Ad Revenue Proceeds (Unbaked)"
          label: "D330 Ad Revenue (Unbaked)"
          description: "Unbaked - Cumulative sum from life to D[x] (src = ironSource)"
          value_format_name: usd_0
          sql: SUM(CASE
              WHEN ${TABLE}.cohort_age >= 330 THEN ${TABLE}.d330_ad_revenue
              WHEN ${TABLE}.cohort_age < 330 THEN ${TABLE}.ad_revenue
              ELSE NULL
          END
          ) ;;
        }

        measure: d360_ad_revenue {
          type: number
          view_label: "Cumulative D[x] - Ad Mon"
          group_label: "Ad Revenue Proceeds"
          label: "D360 Ad Revenue"
          description: "Cumulative sum from life to D[x] (src = ironSource)"
          value_format_name: usd_0
          sql: COALESCE(SUM(${TABLE}.d330_ad_revenue),0) ;;
        }

        measure: d360_ad_revenue_unbaked {
          type: number
          view_label: "Cumulative D[x] - Ad Mon"
          group_label: "Ad Revenue Proceeds (Unbaked)"
          label: "D360 Ad Revenue (Unbaked)"
          description: "Unbaked - Cumulative sum from life to D[x] (src = ironSource)"
          value_format_name: usd_0
          sql: SUM(CASE
            WHEN ${TABLE}.cohort_age >= 360 THEN ${TABLE}.d360_ad_revenue
            WHEN ${TABLE}.cohort_age < 360 THEN ${TABLE}.ad_revenue
            ELSE NULL
        END
        ) ;;

          }
    # Ad Revenue Offerwall D[x] Rolling Sum
      measure: d0_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D00 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d0_ad_revenue_offerwall),0) ;;
      }

      measure: d1_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D01 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d1_ad_revenue_offerwall),0) ;;
      }

      measure: d3_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D03 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d3_ad_revenue_offerwall),0) ;;
      }

      measure: d7_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D07 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d7_ad_revenue_offerwall),0) ;;
      }

      measure: d14_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D14 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d14_ad_revenue_offerwall),0) ;;
      }

      measure: d15_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D15 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d15_ad_revenue_offerwall),0) ;;
      }

      measure: d21_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D21 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d21_ad_revenue_offerwall),0) ;;
      }

      measure: d30_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D30 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d30_ad_revenue_offerwall),0) ;;
      }

      measure: d60_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D60 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d60_ad_revenue_offerwall),0) ;;
      }

      measure: d90_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D90 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d90_ad_revenue_offerwall),0) ;;
      }

      measure: d120_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D120 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d120_ad_revenue_offerwall),0) ;;
      }

      measure: d150_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D150 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d150_ad_revenue_offerwall),0) ;;
      }

      measure: d180_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D180 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d180_ad_revenue_offerwall),0) ;;
      }

      measure: d210_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D210 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d210_ad_revenue_offerwall),0) ;;
      }

      measure: d240_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D240 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d240_ad_revenue_offerwall),0) ;;
      }

      measure: d270_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D270 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d270_ad_revenue_offerwall),0) ;;
      }

      measure: d300_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D300 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d300_ad_revenue_offerwall),0) ;;
      }

      measure: d330_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D330 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d330_ad_revenue_offerwall),0) ;;
      }

      measure: d360_ad_revenue_offerwall {
        type: number
        view_label: "Cumulative D[x] - Ad Mon"
        group_label: "Ad Revenue Proceeds"
        label: "D360 Ad Revenue Offerwall"
        description: "Cumulative sum from life to D[x] for ad unit Offerwall(src = tapjoy)"
        value_format_name: usd_0
        sql: COALESCE(SUM(${TABLE}.d360_ad_revenue_offerwall),0) ;;
      }
        # Ad Revenue Non-Offerwall D[x] Rolling Sum
          measure: d0_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D00 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d0_ad_revenue} - ${d0_ad_revenue_offerwall},0) ;;
          }

          measure: d1_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D01 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d1_ad_revenue} - ${d1_ad_revenue_offerwall},0) ;;
          }

          measure: d3_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D03 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d3_ad_revenue} - ${d3_ad_revenue_offerwall},0) ;;
          }

          measure: d7_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D07 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d7_ad_revenue} - ${d7_ad_revenue_offerwall},0) ;;
          }

          measure: d14_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D14 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d14_ad_revenue} - ${d14_ad_revenue_offerwall},0) ;;
          }

          measure: d15_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D15 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d15_ad_revenue} - ${d15_ad_revenue_offerwall},0) ;;
          }

          measure: d21_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D21 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d21_ad_revenue} - ${d21_ad_revenue_offerwall},0) ;;
          }

          measure: d30_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D30 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d30_ad_revenue} - ${d30_ad_revenue_offerwall},0) ;;
          }

          measure: d60_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D60 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d60_ad_revenue} - ${d60_ad_revenue_offerwall},0) ;;
          }

          measure: d90_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D90 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d90_ad_revenue} - ${d90_ad_revenue_offerwall},0) ;;
          }

          measure: d120_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D120 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d120_ad_revenue} - ${d120_ad_revenue_offerwall},0) ;;
          }

          measure: d150_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D150 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d150_ad_revenue} - ${d150_ad_revenue_offerwall},0) ;;
          }

          measure: d180_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D180 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d180_ad_revenue} - ${d180_ad_revenue_offerwall},0) ;;
          }

          measure: d210_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D210 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d210_ad_revenue} - ${d210_ad_revenue_offerwall},0) ;;
          }

          measure: d240_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D240 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d240_ad_revenue} - ${d240_ad_revenue_offerwall},0) ;;
          }

          measure: d270_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D270 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d270_ad_revenue} - ${d270_ad_revenue_offerwall},0) ;;
          }

          measure: d300_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D300 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d300_ad_revenue} - ${d300_ad_revenue_offerwall},0) ;;
          }

          measure: d330_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D330 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d330_ad_revenue} - ${d330_ad_revenue_offerwall},0) ;;
          }

          measure: d360_ad_revenue_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Revenue Proceeds"
            label: "D360 Ad Revenue Non-Offerwall"
            description: "Cumulative sum from life to D[x] for ad unit Non-Offerwall (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE(${d360_ad_revenue} - ${d360_ad_revenue_offerwall},0) ;;
          }
        # Ad Impressions D[x] Rolling Sum

          measure: d0_ad_impressions {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Impressions"
            label: "D00 Ad Impressions"
            description: "Cumulative sum of ad impressions from life to D[x] (src = ironSource)"
            value_format_name: decimal_0
            sql: COALESCE(SUM(${TABLE}.d0_ad_impressions),0) ;;
          }

          measure: d1_ad_impressions {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Impressions"
            label: "D01 Ad Impressions"
            description: "Cumulative sum of ad impressions from life to D[x] (src = ironSource)"
            value_format_name: decimal_0
            sql: COALESCE(SUM(${TABLE}.d1_ad_impressions),0) ;;
          }

          measure: d3_ad_impressions {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Impressions"
            label: "D03 Ad Impressions"
            description: "Cumulative sum of ad impressions from life to D[x] (src = ironSource)"
            value_format_name: decimal_0
            sql: COALESCE(SUM(${TABLE}.d3_ad_impressions),0) ;;
          }

          measure: d7_ad_impressions {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Impressions"
            label: "D07 Ad Impressions"
            description: "Cumulative sum of ad impressions from life to D[x] (src = ironSource)"
            value_format_name: decimal_0
            sql: COALESCE(SUM(${TABLE}.d7_ad_impressions),0) ;;
          }

          measure: d14_ad_impressions {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Impressions"
            label: "D14 Ad Impressions"
            description: "Cumulative sum of ad impressions from life to D[x] (src = ironSource)"
            value_format_name: decimal_0
            sql: COALESCE(SUM(${TABLE}.d14_ad_impressions),0) ;;
          }

          measure: d21_ad_impressions {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Impressions"
            label: "D21 Ad Impressions"
            description: "Cumulative sum of ad impressions from life to D[x] (src = ironSource)"
            value_format_name: decimal_0
            sql: COALESCE(SUM(${TABLE}.d21_ad_impressions),0) ;;
          }

          measure: d30_ad_impressions {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Impressions"
            label: "D30 Ad Impressions"
            description: "Cumulative sum of ad impressions from life to D[x] (src = ironSource)"
            value_format_name: decimal_0
            sql: COALESCE(SUM(${TABLE}.d30_ad_impressions),0) ;;
          }

          measure: d60_ad_impressions {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Impressions"
            label: "D60 Ad Impressions"
            description: "Cumulative sum of ad impressions from life to D[x] (src = ironSource)"
            value_format_name: decimal_0
            sql: COALESCE(SUM(${TABLE}.d60_ad_impressions),0) ;;
          }

          measure: d90_ad_impressions {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad Impressions"
            label: "D90 Ad Impressions"
            description: "Cumulative sum of ad impressions from life to D[x] (src = ironSource)"
            value_format_name: decimal_0
            sql: COALESCE(SUM(${TABLE}.d90_ad_impressions),0) ;;
          }

          # Cumulative D[x] Ad Mon ARPU

          measure: d0_ad_mon_arpu {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ARPU"
            label: "D00 ARPU"
            description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${d0_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d0_cohort_size}),0)::FLOAT,0) ;;
          }

          measure: d1_ad_mon_arpu {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ARPU"
            label: "D01 ARPU"
            description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${d1_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d1_cohort_size}),0)::FLOAT,0) ;;
          }

          measure: d3_ad_mon_arpu {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ARPU"
            label: "D03 ARPU"
            description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${d3_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d3_cohort_size}),0)::FLOAT,0) ;;
          }

          measure: d7_ad_mon_arpu {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ARPU"
            label: "D07 ARPU"
            description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${d7_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d7_cohort_size}),0)::FLOAT,0) ;;
          }

          measure: d14_ad_mon_arpu {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ARPU"
            label: "D14 ARPU"
            description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${d14_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d14_cohort_size}),0)::FLOAT,0) ;;
          }

          # measure: d15_ad_mon_arpu {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label: "D15 ARPU"
          #   description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d15_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d15_cohort_size}),0)::FLOAT,0) ;;
          # }

          measure: d21_ad_mon_arpu {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ARPU"
            label: "D21 ARPU"
            description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${d21_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d21_cohort_size}),0)::FLOAT,0) ;;
          }

          measure: d30_ad_mon_arpu {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ARPU"
            label: "D30 ARPU"
            description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${d30_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d30_cohort_size}),0)::FLOAT,0) ;;
          }

          measure: d60_ad_mon_arpu {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ARPU"
            label: "D60 ARPU"
            description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${d60_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d60_cohort_size}),0)::FLOAT,0) ;;
          }

          measure: d90_ad_mon_arpu {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ARPU"
            label: "D90 ARPU"
            description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${d90_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d90_cohort_size}),0)::FLOAT,0) ;;
          }

          #arpu from d120 to d360 (d15_cohort_size and dx_cohort_size (x from 120 to 360) are not there in campaign_cohort_device_activity view)

          # measure: d120_ad_mon_arpu {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label:"D120 ARPU"
          #   description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d120_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d120_cohort_size}),0)::FLOAT,0);;
          # }

          # measure: d150_ad_mon_arpu {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label:"D150 ARPU"
          #   description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d150_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d150_cohort_size}),0)::FLOAT,0);;
          # }

          # measure: d180_ad_mon_arpu {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label:"D180 ARPU"
          #   description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d180_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d180_cohort_size}),0)::FLOAT,0);;
          # }

          # measure: d210_ad_mon_arpu {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label:"D210 ARPU"
          #   description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d210_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d210_cohort_size}),0)::FLOAT,0);;
          # }

          # measure: d240_ad_mon_arpu {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label:"D240 ARPU"
          #   description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d240_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d240_cohort_size}),0)::FLOAT,0);;
          # }

          # measure: d270_ad_mon_arpu {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label:"D270 ARPU"
          #   description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d270_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d270_cohort_size}),0)::FLOAT,0);;
          # }

          # measure: d300_ad_mon_arpu {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label:"D300 ARPU"
          #   description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d300_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d300_cohort_size}),0)::FLOAT,0);;
          # }

          # measure: d330_ad_mon_arpu {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label:"D330 ARPU"
          #   description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d330_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d330_cohort_size}),0)::FLOAT,0);;
          # }

          # measure: d360_ad_mon_arpu {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label:"D360 ARPU"
          #   description: "D[x] Ad Revenue (src = ironSource) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d360_ad_revenue})/NULLIF((${campaign_cohort_device_activity.d360_cohort_size}),0)::FLOAT,0);;
          # }

          # ARPU Offerwall

          measure: d0_ad_mon_arpu_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ARPU"
            label: "D00 ARPU Offerwall"
            description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${d0_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d0_cohort_size}),0)::FLOAT,0);;
          }

          measure: d1_ad_mon_arpu_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ARPU"
            label: "D01 ARPU Offerwall"
            description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${d1_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d1_cohort_size}),0)::FLOAT,0);;
          }

          measure: d3_ad_mon_arpu_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ARPU"
            label: "D03 ARPU Offerwall"
            description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${d3_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d3_cohort_size}),0)::FLOAT,0);;
          }

          measure: d7_ad_mon_arpu_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ARPU"
            label: "D07 ARPU Offerwall"
            description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${d7_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d7_cohort_size}),0)::FLOAT,0);;
          }

          measure: d14_ad_mon_arpu_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ARPU"
            label: "D14 ARPU Offerwall"
            description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${d14_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d14_cohort_size}),0)::FLOAT,0);;
          }

          # measure: d15_ad_mon_arpu_offerwall {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label: "D15 ARPU Offerwall"
          #   description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d15_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d15_cohort_size}),0)::FLOAT,0);;
          # }

          measure: d21_ad_mon_arpu_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ARPU"
            label: "D21 ARPU Offerwall"
            description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${d21_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d21_cohort_size}),0)::FLOAT,0);;
          }

          measure: d30_ad_mon_arpu_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ARPU"
            label: "D30 ARPU Offerwall"
            description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${d30_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d30_cohort_size}),0)::FLOAT,0);;
          }

          measure: d60_ad_mon_arpu_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ARPU"
            label: "D60 ARPU Offerwall"
            description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${d60_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d60_cohort_size}),0)::FLOAT,0);;
          }

          measure: d90_ad_mon_arpu_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ARPU"
            label: "D90 ARPU Offerwall"
            description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${d90_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d90_cohort_size}),0)::FLOAT,0);;
          }

          # measure: d120_ad_mon_arpu_offerwall {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label: "D120 ARPU Offerwall"
          #   description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d120_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d120_cohort_size}),0)::FLOAT,0);;
          # }

          # measure: d150_ad_mon_arpu_offerwall {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label: "D150 ARPU Offerwall"
          #   description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d150_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d150_cohort_size}),0)::FLOAT,0);;
          # }

          # measure: d180_ad_mon_arpu_offerwall {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label: "D180 ARPU Offerwall"
          #   description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d180_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d180_cohort_size}),0)::FLOAT,0);;
          # }

          # measure: d210_ad_mon_arpu_offerwall {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label: "D210 ARPU Offerwall"
          #   description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d210_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d210_cohort_size}),0)::FLOAT,0);;
          # }

          # measure: d240_ad_mon_arpu_offerwall {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label: "D240 ARPU Offerwall"
          #   description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d240_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d240_cohort_size}),0)::FLOAT,0);;
          # }

          # measure: d270_ad_mon_arpu_offerwall {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label: "D270 ARPU Offerwall"
          #   description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d270_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d270_cohort_size}),0)::FLOAT,0);;
          # }

          # measure: d300_ad_mon_arpu_offerwall {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label: "D300 ARPU Offerwall"
          #   description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d300_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d300_cohort_size}),0)::FLOAT,0);;
          # }

          # measure: d330_ad_mon_arpu_offerwall {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label: "D330 ARPU Offerwall"
          #   description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d330_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d330_cohort_size}),0)::FLOAT,0);;
          # }

          # measure: d360_ad_mon_arpu_offerwall {
          #   type: number
          #   view_label: "Cumulative D[x] - Ad Mon"
          #   group_label: "Ad ARPU"
          #   label: "D360 ARPU Offerwall"
          #   description: "D[x] Ad Revenue Offerwall (src = tapjoy) / D[x] Installs (src = MMP)"
          #   value_format_name: usd
          #   sql: COALESCE((${d360_ad_revenue_offerwall})/NULLIF((${campaign_cohort_device_activity.d360_cohort_size}),0)::FLOAT,0);;
          # }

          # ARPU Non-Offerwall

          measure: d0_ad_mon_arpu_non_offerwall {
              type: number
              view_label: "Cumulative D[x] - Ad Mon"
              group_label: "Ad ARPU"
              label: "D00 ARPU Non-Offerwall"
              description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
              value_format_name: usd
              sql: COALESCE((${d0_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d0_cohort_size}),0)::FLOAT,0);;
            }

            measure: d1_ad_mon_arpu_non_offerwall {
              type: number
              view_label: "Cumulative D[x] - Ad Mon"
              group_label: "Ad ARPU"
              label: "D01 ARPU Non-Offerwall"
              description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
              value_format_name: usd
              sql: COALESCE((${d1_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d1_cohort_size}),0)::FLOAT,0);;
            }

            measure: d3_ad_mon_arpu_non_offerwall {
              type: number
              view_label: "Cumulative D[x] - Ad Mon"
              group_label: "Ad ARPU"
              label: "D03 ARPU Non-Offerwall"
              description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
              value_format_name: usd
              sql: COALESCE((${d3_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d3_cohort_size}),0)::FLOAT,0);;
            }

            measure: d7_ad_mon_arpu_non_offerwall {
              type: number
              view_label: "Cumulative D[x] - Ad Mon"
              group_label: "Ad ARPU"
              label: "D07 ARPU Non-Offerwall"
              description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
              value_format_name: usd
              sql: COALESCE((${d7_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d7_cohort_size}),0)::FLOAT,0);;
            }

            measure: d14_ad_mon_arpu_non_offerwall {
              type: number
              view_label: "Cumulative D[x] - Ad Mon"
              group_label: "Ad ARPU"
              label: "D14 ARPU Non-Offerwall"
              description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
              value_format_name: usd
              sql: COALESCE((${d14_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d14_cohort_size}),0)::FLOAT,0);;
            }

            # measure: d15_ad_mon_arpu_non_offerwall {
            #   type: number
            #   view_label: "Cumulative D[x] - Ad Mon"
            #   group_label: "Ad ARPU"
            #   label: "D15 ARPU Non-Offerwall"
            #   description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
            #   value_format_name: usd
            #   sql: COALESCE((${d15_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d15_cohort_size}),0)::FLOAT,0);;
            # }

            measure: d21_ad_mon_arpu_non_offerwall {
              type: number
              view_label: "Cumulative D[x] - Ad Mon"
              group_label: "Ad ARPU"
              label: "D21 ARPU Non-Offerwall"
              description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
              value_format_name: usd
              sql: COALESCE((${d21_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d21_cohort_size}),0)::FLOAT,0);;
            }

            measure: d30_ad_mon_arpu_non_offerwall {
              type: number
              view_label: "Cumulative D[x] - Ad Mon"
              group_label: "Ad ARPU"
              label: "D30 ARPU Non-Offerwall"
              description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
              value_format_name: usd
              sql: COALESCE((${d30_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d30_cohort_size}),0)::FLOAT,0);;
            }

            measure: d60_ad_mon_arpu_non_offerwall {
              type: number
              view_label: "Cumulative D[x] - Ad Mon"
              group_label: "Ad ARPU"
              label: "D60 ARPU Non-Offerwall"
              description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
              value_format_name: usd
              sql: COALESCE((${d60_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d60_cohort_size}),0)::FLOAT,0);;
            }

            measure: d90_ad_mon_arpu_non_offerwall {
              type: number
              view_label: "Cumulative D[x] - Ad Mon"
              group_label: "Ad ARPU"
              label: "D90 ARPU Non-Offerwall"
              description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
              value_format_name: usd
              sql: COALESCE((${d90_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d90_cohort_size}),0)::FLOAT,0);;
            }

            # measure: d120_ad_mon_arpu_non_offerwall {
            #   type: number
            #   view_label: "Cumulative D[x] - Ad Mon"
            #   group_label: "Ad ARPU"
            #   label: "D120 ARPU Non-Offerwall"
            #   description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
            #   value_format_name: usd
            #   sql: COALESCE((${d120_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d120_cohort_size}),0)::FLOAT,0);;
            # }

            # measure: d150_ad_mon_arpu_non_offerwall {
            #   type: number
            #   view_label: "Cumulative D[x] - Ad Mon"
            #   group_label: "Ad ARPU"
            #   label: "D150 ARPU Non-Offerwall"
            #   description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
            #   value_format_name: usd
            #   sql: COALESCE((${d150_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d150_cohort_size}),0)::FLOAT,0);;
            # }

            # measure: d180_ad_mon_arpu_non_offerwall {
            #   type: number
            #   view_label: "Cumulative D[x] - Ad Mon"
            #   group_label: "Ad ARPU"
            #   label: "D180 ARPU Non-Offerwall"
            #   description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
            #   value_format_name: usd
            #   sql: COALESCE((${d180_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d180_cohort_size}),0)::FLOAT,0);;
            # }

            # measure: d210_ad_mon_arpu_non_offerwall {
            #   type: number
            #   view_label: "Cumulative D[x] - Ad Mon"
            #   group_label: "Ad ARPU"
            #   label: "D210 ARPU Non-Offerwall"
            #   description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
            #   value_format_name: usd
            #   sql: COALESCE((${d210_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d210_cohort_size}),0)::FLOAT,0);;
            # }

            # measure: d240_ad_mon_arpu_non_offerwall {
            #   type: number
            #   view_label: "Cumulative D[x] - Ad Mon"
            #   group_label: "Ad ARPU"
            #   label: "D240 ARPU Non-Offerwall"
            #   description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
            #   value_format_name: usd
            #   sql: COALESCE((${d240_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d240_cohort_size}),0)::FLOAT,0);;
            # }

            # measure: d270_ad_mon_arpu_non_offerwall {
            #   type: number
            #   view_label: "Cumulative D[x] - Ad Mon"
            #   group_label: "Ad ARPU"
            #   label: "D270 ARPU Non-Offerwall"
            #   description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
            #   value_format_name: usd
            #   sql: COALESCE((${d270_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d270_cohort_size}),0)::FLOAT,0);;
            # }

            # measure: d300_ad_mon_arpu_non_offerwall {
            #   type: number
            #   view_label: "Cumulative D[x] - Ad Mon"
            #   group_label: "Ad ARPU"
            #   label: "D300 ARPU Non-Offerwall"
            #   description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
            #   value_format_name: usd
            #   sql: COALESCE((${d300_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d300_cohort_size}),0)::FLOAT,0);;
            # }

            # measure: d330_ad_mon_arpu_non_offerwall {
            #   type: number
            #   view_label: "Cumulative D[x] - Ad Mon"
            #   group_label: "Ad ARPU"
            #   label: "D330 ARPU Non-Offerwall"
            #   description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
            #   value_format_name: usd
            #   sql: COALESCE((${d330_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d330_cohort_size}),0)::FLOAT,0);;
            # }

            # measure: d360_ad_mon_arpu_non_offerwall {
            #   type: number
            #   view_label: "Cumulative D[x] - Ad Mon"
            #   group_label: "Ad ARPU"
            #   label: "D360 ARPU Non-Offerwall"
            #   description: "D[x] Ad Revenue Non-Offerwall (src = ironSource) / D[x] Installs (src = MMP)"
            #   value_format_name: usd
            #   sql: COALESCE((${d360_ad_revenue_non_offerwall})/NULLIF((${campaign_cohort_device_activity.d360_cohort_size}),0)::FLOAT,0);;
            # }

## Singular Metrics - If this is not joined to Singular, we need to exclude these using the "fields" Explore parameter.

          # Cost Per Ad Engaged User
          # I put this here to keep the singular dependent metrics together.
          measure: cost_per_ad_engaged_user {
            type: number
            view_label: "Singular"
            group_label: "Ad Monetization"
            label: "Cost Per Ad Engaged User"
            description: "Cost / Ad Engaged Users (src = MMP)"
            value_format_name: usd_0
            sql: COALESCE(${campaign_cohort_cost.adn_cost} / NULLIF(${ad_engaged_users},0)::FLOAT,0) ;;
          }

          measure: d0_cost_per_ad_engaged_user {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Cost Per Ad Engaged User"
            label: "D00 Cost per Ad Engaged User"
            description: "Cost / D[x] Ad Engaged Users (src = MMP)"
            value_format_name: usd_0
            sql: COALESCE(${campaign_cohort_cost.adn_cost} / NULLIF(${d0_ad_engaged_users},0)::FLOAT,0) ;;
          }

          measure: d1_cost_per_ad_engaged_user {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Cost Per Ad Engaged User"
            label: "D01 Cost per Ad Engaged User"
            description: "Cost / D[x] Ad Engaged Users (src = MMP)"
            value_format_name: usd_0
            sql: COALESCE(${campaign_cohort_cost.adn_cost} / NULLIF(${d1_ad_engaged_users},0)::FLOAT,0) ;;
          }

          measure: d3_cost_per_ad_engaged_user {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Cost Per Ad Engaged User"
            label: "D03 Cost per Ad Engaged User"
            description: "Cost / D[x] Ad Engaged Users (src = MMP)"
            value_format_name: usd_0
            sql: COALESCE(${campaign_cohort_cost.adn_cost} / NULLIF(${d3_ad_engaged_users},0)::FLOAT,0) ;;
          }

          measure: d7_cost_per_ad_engaged_user {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Cost Per Ad Engaged User"
            label: "D07 Cost per Ad Engaged User"
            description: "Cost / D[x] Ad Engaged Users (src = MMP)"
            value_format_name: usd_0
            sql: COALESCE(${campaign_cohort_cost.adn_cost} / NULLIF(${d7_ad_engaged_users},0)::FLOAT,0) ;;
          }

          measure: d14_cost_per_ad_engaged_user {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Cost Per Ad Engaged User"
            label: "D14 Cost per Ad Engaged User"
            description: "Cost / D[x] Ad Engaged Users (src = MMP)"
            value_format_name: usd_0
            sql: COALESCE(${campaign_cohort_cost.adn_cost} / NULLIF(${d14_ad_engaged_users},0)::FLOAT,0) ;;
          }

          measure: d21_cost_per_ad_engaged_user {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Cost Per Ad Engaged User"
            label: "D21 Cost per Ad Engaged User"
            description: "Cost / D[x] Ad Engaged Users (src = MMP)"
            value_format_name: usd_0
            sql: COALESCE(${campaign_cohort_cost.adn_cost} / NULLIF(${d21_ad_engaged_users},0)::FLOAT,0) ;;
          }

          measure: d30_cost_per_ad_engaged_user {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Cost Per Ad Engaged User"
            label: "D30 Cost per Ad Engaged User"
            description: "Cost / D[x] Ad Engaged Users (src = MMP)"
            value_format_name: usd_0
            sql: COALESCE(${campaign_cohort_cost.adn_cost} / NULLIF(${d30_ad_engaged_users},0)::FLOAT,0) ;;
          }

          measure: d60_cost_per_ad_engaged_user {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Cost Per Ad Engaged User"
            label: "D60 Cost per Ad Engaged User"
            description: "Cost / D[x] Ad Engaged Users (src = MMP)"
            value_format_name: usd_0
            sql: COALESCE(${campaign_cohort_cost.adn_cost} / NULLIF(${d60_ad_engaged_users},0)::FLOAT,0) ;;
          }

          measure: d90_cost_per_ad_engaged_user {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Cost Per Ad Engaged User"
            label: "D90 Cost per Ad Engaged User"
            description: "Cost / D[x] Ad Engaged Users (src = MMP)"
            value_format_name: usd_0
            sql: COALESCE(${campaign_cohort_cost.adn_cost} / NULLIF(${d90_ad_engaged_users},0)::FLOAT,0) ;;
          }

          # CPAI - Cost per Ad Impression --instead of CPIAP (Cost Per In App Purchase)
          # I put this here to keep the singular dependent metrics together.
          measure: cpai {
            type: number
            view_label: "Singular"
            group_label: "Ad Monetization"
            label: "CPAI"
            description: "Cost / Ad Impressions (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE((${campaign_cohort_cost.adn_cost} / NULLIF(${ad_impressions},0)::FLOAT),0) ;;
          }

          measure: d0_cpai {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "CPAI"
            label: "D00 CPAI"
            description: "Cost / D[x] Ad Impressions (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE((${campaign_cohort_cost.adn_cost} / NULLIF(${d0_ad_impressions},0)::FLOAT),0) ;;
          }

          measure: d1_cpai {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "CPAI"
            label: "D01 CPAI"
            description: "Cost / D[x] Ad Impressions (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE((${campaign_cohort_cost.adn_cost} / NULLIF(${d1_ad_impressions},0)::FLOAT),0) ;;
          }

          measure: d3_cpai {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "CPAI"
            label: "D03 CPAI"
            description: "Cost / D[x] Ad - Impressions (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE((${campaign_cohort_cost.adn_cost} / NULLIF(${d3_ad_impressions},0)::FLOAT),0) ;;
          }

          measure: d7_cpai {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "CPAI"
            label: "D07 CPAI"
            description: "Cost / D[x] Ad Impressions (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE((${campaign_cohort_cost.adn_cost} / NULLIF(${d7_ad_impressions},0)::FLOAT),0) ;;
          }

          measure: d14_cpai {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "CPAI"
            label: "D14 CPAI"
            description: "Cost / D[x] Ad Impressions (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE((${campaign_cohort_cost.adn_cost} / NULLIF(${d14_ad_impressions},0)::FLOAT),0) ;;
          }

          measure: d21_cpai {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "CPAI"
            label: "D21 CPAI"
            description: "Cost / D[x] Ad Impressions (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE((${campaign_cohort_cost.adn_cost} / NULLIF(${d21_ad_impressions},0)::FLOAT),0) ;;
          }

          measure: d30_cpai {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "CPAI"
            label: "D30 CPAI"
            description: "Cost / D[x] Ad Impressions (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE((${campaign_cohort_cost.adn_cost} / NULLIF(${d30_ad_impressions},0)::FLOAT),0) ;;
          }

          measure: d60_cpai {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "CPAI"
            label: "D60 CPAI"
            description: "Cost / D[x] Ad Impressions (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE((${campaign_cohort_cost.adn_cost} / NULLIF(${d60_ad_impressions},0)::FLOAT),0) ;;
          }

          measure: d90_cpai {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "CPAI"
            label: "D90 CPAI"
            description: "Cost / D[x] Ad Impressions (src = ironSource)"
            value_format_name: usd_0
            sql: COALESCE((${campaign_cohort_cost.adn_cost} / NULLIF(${d90_ad_impressions},0)::FLOAT),0) ;;
          }

          # Ad Mon ROAS - Return on Ad Spend

          # I put this here to keep the singular dependent metrics together.
          measure: ad_mon_roas {
            type: number
            view_label: "Singular"
            group_label: "Ad Monetization"
            label: "Ad ROAS"
            description: "Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${ad_revenue} / NULLIF(${campaign_cohort_cost.adn_cost},0)::FLOAT),0) ;;
          }

          measure: ad_mon_roas_offerwall {
            type: number
            view_label: "Singular"
            group_label: "Ad Monetization"
            label: "Ad ROAS Offerwall"
            description: "Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.adn_cost},0)::FLOAT),0) ;;
          }

          measure: ad_mon_roas_non_offerwall {
            type: number
            view_label: "Singular"
            group_label: "Ad Monetization"
            label: "Ad ROAS Non-Offerwall"
            description: "Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${ad_revenue_non_offerwall}/ NULLIF(${campaign_cohort_cost.adn_cost},0)::FLOAT),0) ;;
          }

       # ROAS D[X]

          measure: d0_ad_mon_roas {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D00 ROAS"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d0_ad_revenue} / NULLIF(${campaign_cohort_cost.adn_cost},0)::FLOAT),0) ;;
          }

          measure: d1_ad_mon_roas {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D01 ROAS"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d1_ad_revenue} / NULLIF(${campaign_cohort_cost.d1_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d3_ad_mon_roas {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D03 ROAS"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d3_ad_revenue} / NULLIF(${campaign_cohort_cost.d3_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d7_ad_mon_roas {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D07 ROAS"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d7_ad_revenue} / NULLIF(${campaign_cohort_cost.d7_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d14_ad_mon_roas {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D14 ROAS"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d14_ad_revenue} / NULLIF(${campaign_cohort_cost.d14_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d15_ad_mon_roas {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D15 ROAS"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d15_ad_revenue} / NULLIF(${campaign_cohort_cost.d15_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d30_ad_mon_roas {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D30 ROAS"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d30_ad_revenue} / NULLIF(${campaign_cohort_cost.d30_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d60_ad_mon_roas {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D60 ROAS"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d60_ad_revenue} / NULLIF(${campaign_cohort_cost.d60_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d90_ad_mon_roas {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D90 ROAS"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d90_ad_revenue} / NULLIF(${campaign_cohort_cost.d90_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d120_ad_mon_roas {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D120 ROAS"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d120_ad_revenue} / NULLIF(${campaign_cohort_cost.d120_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d150_ad_mon_roas {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D150 ROAS"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d150_ad_revenue} / NULLIF(${campaign_cohort_cost.d150_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d180_ad_mon_roas {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D180 ROAS"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d180_ad_revenue} / NULLIF(${campaign_cohort_cost.d180_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d210_ad_mon_roas {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D210 ROAS"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d210_ad_revenue} / NULLIF(${campaign_cohort_cost.d210_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d240_ad_mon_roas {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D240 ROAS"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d240_ad_revenue} / NULLIF(${campaign_cohort_cost.d240_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d270_ad_mon_roas {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D270 ROAS"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d270_ad_revenue} / NULLIF(${campaign_cohort_cost.d270_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d300_ad_mon_roas {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D300 ROAS"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d300_ad_revenue} / NULLIF(${campaign_cohort_cost.d300_adn_cost},0)::FLOAT),0) ;;
          }
          measure: d330_ad_mon_roas {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D330 ROAS"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d330_ad_revenue} / NULLIF(${campaign_cohort_cost.d330_adn_cost},0)::FLOAT),0) ;;
          }
          measure: d360_ad_mon_roas {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D360 ROAS"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d360_ad_revenue} / NULLIF(${campaign_cohort_cost.d360_adn_cost},0)::FLOAT),0) ;;
          }

          # ROAS D[X] Offerwall

          measure: d0_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D00 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d0_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.adn_cost},0)::FLOAT),0);;
          }

          measure: d1_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D01 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d1_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.d1_adn_cost},0)::FLOAT),0);;
          }

          measure: d3_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D03 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d3_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.d3_adn_cost},0)::FLOAT),0);;
          }

          measure: d7_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D07 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d7_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.d7_adn_cost},0)::FLOAT),0);;
          }

          measure: d14_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D14 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d14_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.d14_adn_cost},0)::FLOAT),0);;
          }

          measure: d15_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D15 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d15_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.d15_adn_cost},0)::FLOAT),0);;
          }

          measure: d21_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D21 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d21_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.d21_adn_cost},0)::FLOAT),0);;
          }

          measure: d30_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D30 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d30_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.d30_adn_cost},0)::FLOAT),0);;
          }

          measure: d60_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D60 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d60_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.d60_adn_cost},0)::FLOAT),0);;
          }

          measure: d90_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D90 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d90_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.d90_adn_cost},0)::FLOAT),0);;
          }

          measure: d120_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D120 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d120_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.d120_adn_cost},0)::FLOAT),0);;
          }

          measure: d150_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D150 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d150_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.d150_adn_cost},0)::FLOAT),0);;
          }

          measure: d180_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D180 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d180_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.d180_adn_cost},0)::FLOAT),0);;
          }

          measure: d210_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D210 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d210_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.d210_adn_cost},0)::FLOAT),0);;
          }

          measure: d240_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D240 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d240_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.d240_adn_cost},0)::FLOAT),0);;
          }

          measure: d270_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D270 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d270_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.d270_adn_cost},0)::FLOAT),0);;
          }

          measure: d300_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D300 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d300_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.d300_adn_cost},0)::FLOAT),0);;
          }

          measure: d330_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D330 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d330_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.d330_adn_cost},0)::FLOAT),0);;
          }

          measure: d360_ad_mon_roas_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D360 ROAS Offerwall"
            description: "D[x] Ad Revenue (src = tapjoy) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d360_ad_revenue_offerwall} / NULLIF(${campaign_cohort_cost.d360_adn_cost},0)::FLOAT),0);;
          }

          # ROAS D[X] Non-Offerwall

          measure: d0_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D00 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d0_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.adn_cost},0)::FLOAT),0);;
          }

          measure: d1_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D01 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d1_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.d1_adn_cost},0)::FLOAT),0);;
          }

          measure: d3_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D03 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d3_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.d3_adn_cost},0)::FLOAT),0);;
          }

          measure: d7_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D07 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d7_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.d7_adn_cost},0)::FLOAT),0);;
          }

          measure: d14_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D14 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d14_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.d14_adn_cost},0)::FLOAT),0);;
          }

          measure: d15_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D15 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d15_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.d15_adn_cost},0)::FLOAT),0);;
          }

          measure: d21_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D21 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d21_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.d21_adn_cost},0)::FLOAT),0);;
          }

          measure: d30_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D30 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d30_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.d30_adn_cost},0)::FLOAT),0);;
          }

          measure: d60_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D60 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d60_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.d60_adn_cost},0)::FLOAT),0);;
          }

          measure: d90_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D90 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d90_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.d90_adn_cost},0)::FLOAT),0);;
          }

          measure: d120_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D120 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d120_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.d120_adn_cost},0)::FLOAT),0);;
          }

          measure: d150_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D150 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d150_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.d150_adn_cost},0)::FLOAT),0);;
          }

          measure: d180_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D180 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d180_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.d180_adn_cost},0)::FLOAT),0);;
          }

          measure: d210_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D210 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d210_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.d210_adn_cost},0)::FLOAT),0);;
          }

          measure: d240_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D240 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d240_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.d240_adn_cost},0)::FLOAT),0);;
          }

          measure: d270_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D270 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d270_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.d270_adn_cost},0)::FLOAT),0);;
          }

          measure: d300_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D300 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d300_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.d300_adn_cost},0)::FLOAT),0);;
          }

          measure: d330_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D330 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d330_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.d330_adn_cost},0)::FLOAT),0);;
          }

          measure: d360_ad_mon_roas_non_offerwall {
            type: number
            view_label: "Cumulative D[x] - Ad Mon"
            group_label: "Ad ROAS"
            label: "D360 ROAS Non-Offerwall"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d360_ad_revenue_non_offerwall} / NULLIF(${campaign_cohort_cost.d360_adn_cost},0)::FLOAT),0);;
          }
############################################ Revenue - without NULLS
          measure: d1_ad_revenue_no_nulls {
            label: "D1 Ad Revenue - No Nulls"
            group_label: "Ad Revenue Values"
            view_label: "Weekly Report Calculations"
            type: number
            description: "Cumulative sum from life to D[x] (src = ironSource)"
            value_format_name: usd_0
            sql:  COALESCE (SUM(${TABLE}.d1_ad_revenue),0) ;;
          }

          measure: d3_ad_revenue_no_nulls {
            label: "D3 Ad Revenue - No Nulls"
            group_label: "Ad Revenue Values"
            view_label: "Weekly Report Calculations"
            type: number
            description: "Cumulative sum from life to D[x] (src = ironSource)"
            value_format_name: usd_0
            sql:  COALESCE (SUM(${TABLE}.d3_ad_revenue),0) ;;
          }

          measure: d7_ad_revenue_no_nulls {
            label: "D7 Ad Revenue - No Nulls"
            group_label: "Ad Revenue Values"
            view_label: "Weekly Report Calculations"
            type: number
            description: "Cumulative sum from life to D[x] (src = ironSource)"
            value_format_name: usd_0
            sql:   COALESCE (SUM(${TABLE}.d7_ad_revenue),0) ;;
          }

          measure: d14_ad_revenue_no_nulls {
            label: "D14 Ad Revenue - No Nulls"
            group_label: "Ad Revenue Values"
            view_label: "Weekly Report Calculations"
            type: number
            description: "Cumulative sum from life to D[x] (src = ironSource)"
            value_format_name: usd_0
            sql:  COALESCE (SUM(${TABLE}.d14_ad_revenue),0) ;;
          }

          measure: d21_ad_revenue_no_nulls {
            label: "D21 Ad Revenue - No Nulls"
            group_label: "Ad Revenue Values"
            view_label: "Weekly Report Calculations"
            type: number
            description: "Cumulative sum from life to D[x] (src = ironSource)"
            value_format_name: usd_0
            sql:  COALESCE (SUM(${TABLE}.d21_ad_revenue),0) ;;
          }

          measure: d30_ad_revenue_no_nulls {
            label: "D30 Ad Revenue - No Nulls"
            group_label: "Ad Revenue Values"
            view_label: "Weekly Report Calculations"
            type: number
            description: "Cumulative sum from life to D[x] (src = ironSource)"
            value_format_name: usd_0
            sql:  COALESCE (SUM(${TABLE}.d30_ad_revenue),0) ;;
          }

          measure: d1_ad_mon_roas_no_nulls {
            type: number
            view_label: "Weekly Report Calculations"
            group_label: "Ad Mon ROAS Calculations"
            label: "D1 Ad ROAS - no nulls - %"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d1_ad_revenue_no_nulls} / NULLIF(${campaign_cohort_cost.adn_cost}::int,0)::FLOAT),0) ;;
          }

          measure: d3_ad_mon_roas_no_nulls {
            type: number
            view_label: "Weekly Report Calculations"
            group_label: "Ad Mon ROAS Calculations"
            label: "D3 Ad ROAS - no nulls - %"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d3_ad_revenue_no_nulls} / NULLIF(${campaign_cohort_cost.adn_cost}::int,0)::FLOAT),0) ;;
          }

          measure: d7_ad_mon_roas_No_nulls {
            type: number
            view_label: "Weekly Report Calculations"
            group_label: "Ad Mon ROAS Calculations"
            label: "D7 Ad ROAS - no nulls - %"
            description: "D[x] Ad Revenue (src = ironSource) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d7_ad_revenue_no_nulls} / NULLIF(${campaign_cohort_cost.adn_cost}::int,0)::FLOAT),0) ;;
          }

          #############################################################################################################
          # OVERALL MEASURES
          #############################################################################################################

          measure: overall_net_revenue {
            type: number
            view_label: "Cohort Revenue - Overall"
            label: "Overall Net Revenue"
            description: "Net IAP Revenue and Ad Monetization Revenue. Ad mon revenue is already net when we receive it."
            value_format_name: usd_0
            sql: COALESCE(${ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.net_usd},0) ;;
          }

          measure: avg_overall_net_revenue {
            type: number
            view_label: "Cohort Revenue - Overall"
            label: "Average Overall Net Revenue"
            description: "Average overall net revenue in USD (Of Net IAP Revenue and Ad Monetization Revenue)."
            value_format_name: usd
            sql: COALESCE((${overall_net_revenue} / NULLIF(${origin_campaign_cohort.cohort_size},0)::FLOAT),0) ;;
          }

          measure: overall_net_revenue_arpu {
            type: number
            view_label: "Cohort Revenue - Overall"
            label: "ARPU"
            description: "Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE((${overall_net_revenue} / NULLIF(${origin_campaign_cohort.cohort_size},0)::FLOAT),0) ;;
          }

# Overall Net Revenue D[x] Rolling Sum
          measure: d0_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D00 Overall Net Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d0_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d0_net_usd},0) ;;
          }

          measure: d1_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D01 Overall Net Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d1_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d1_net_usd},0) ;;
          }

          measure: d1_overall_net_revenue_unbaked {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds (Unbaked)"
            label: "D01 Overall Net Revenue (Unbaked)"
            description: "Unbaked - Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: (CASE
                          WHEN SUM(${cohort_age}) >= 1 THEN ${d1_overall_net_revenue}
                          WHEN SUM(${cohort_age}) < 1 THEN ${overall_net_revenue}
                          ELSE NULL
                        END
                  ) ;;
          }

          measure: d3_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D03 Overall Net Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d3_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d3_net_usd},0) ;;
          }

          measure: d3_overall_net_revenue_unbaked {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds (Unbaked)"
            label: "D03 Overall Net Revenue (Unbaked)"
            description: "Unbaked - Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: (CASE
                          WHEN SUM(${cohort_age}) >= 3 THEN ${d3_overall_net_revenue}
                          WHEN SUM(${cohort_age}) < 3 THEN ${overall_net_revenue}
                          ELSE NULL
                        END
                  ) ;;
          }

          measure: d7_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D07 Overall Net Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d7_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d7_net_usd},0) ;;
          }

          measure: d7_overall_net_revenue_unbaked {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds (Unbaked)"
            label: "D07 Overall Net Revenue (Unbaked)"
            description: "Unbaked - Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            # sql: SUM(CASE
            #             WHEN ${TABLE}.cohort_age >= 7 THEN ${TABLE}.d7_overall_net_revenue
            #             WHEN ${TABLE}.cohort_age < 7 THEN ${TABLE}.overall_net_revenue
            #             ELSE NULL
            #         END
            #         ) ;;
            sql: (CASE
                          WHEN SUM(${cohort_age}) >= 7 THEN ${d7_overall_net_revenue}
                          WHEN SUM(${cohort_age}) < 7 THEN ${overall_net_revenue}
                          ELSE NULL
                        END
                  ) ;;
          }

          measure: d14_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D14 Overall Net Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d14_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d14_net_usd},0) ;;
          }

          measure: d14_overall_net_revenue_unbaked {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds (Unbaked)"
            label: "D14 Overall Net Revenue (Unbaked)"
            description: "Unbaked - Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: (CASE
                          WHEN SUM(${cohort_age}) >= 14 THEN ${d14_overall_net_revenue}
                          WHEN SUM(${cohort_age}) < 14 THEN ${overall_net_revenue}
                          ELSE NULL
                        END
                  ) ;;
          }

          measure: d21_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D21 Overall Net Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d21_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d21_net_usd},0) ;;
          }

          measure: d21_overall_net_revenue_unbaked {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds (Unbaked)"
            label: "D21 Overall Net Revenue (Unbaked)"
            description: "Unbaked - Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: (CASE
                          WHEN SUM(${cohort_age}) >= 21 THEN ${d21_overall_net_revenue}
                          WHEN SUM(${cohort_age}) < 21 THEN ${overall_net_revenue}
                          ELSE NULL
                        END
                  ) ;;
          }

          measure: d15_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D15 Overall Net Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d15_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d15_net_usd},0) ;;
          }

          measure: d15_overall_net_revenue_unbaked {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds (Unbaked)"
            label: "D15 Overall Net Revenue (Unbaked)"
            description: "Unbaked - Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: (CASE
                        WHEN SUM(${cohort_age}) >= 15 THEN ${d15_overall_net_revenue}
                        WHEN SUM(${cohort_age}) < 15 THEN ${overall_net_revenue}
                        ELSE NULL
                        END
                  ) ;;
          }

          measure: d30_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D30 Overall Net Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d30_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d30_net_usd},0) ;;
          }

          measure: d30_overall_net_revenue_unbaked {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds (Unbaked)"
            label: "D30 Overall Net Revenue (Unbaked)"
            description: "Unbaked - Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: (CASE
                          WHEN SUM(${cohort_age}) >= 30 THEN ${d30_overall_net_revenue}
                          WHEN SUM(${cohort_age}) < 30 THEN ${overall_net_revenue}
                          ELSE NULL
                        END
                  ) ;;
          }

          measure: d60_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D60 Overall Net Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d60_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d60_net_usd},0) ;;
          }

          measure: d60_overall_net_revenue_unbaked {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds (Unbaked)"
            label: "D60 Overall Net Revenue (Unbaked)"
            description: "Unbaked - Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: (CASE
                          WHEN SUM(${cohort_age}) >= 60 THEN ${d60_overall_net_revenue}
                          WHEN SUM(${cohort_age}) < 60 THEN ${overall_net_revenue}
                          ELSE NULL
                        END
                  ) ;;
          }

          measure: d90_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D90 Overall Net Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d90_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d90_net_usd},0) ;;
          }

          measure: d90_overall_net_revenue_unbaked {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds (Unbaked)"
            label: "D90 Overall Net Revenue (Unbaked)"
            description: "Unbaked - Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: (CASE
                          WHEN SUM(${cohort_age}) >= 90 THEN ${d90_overall_net_revenue}
                          WHEN SUM(${cohort_age}) < 90 THEN ${overall_net_revenue}
                          ELSE NULL
                        END
                  ) ;;
          }

          measure: d120_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D120 Overall Net Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d120_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d120_net_usd},0) ;;
          }

          measure: d120_overall_net_revenue_unbaked {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds (Unbaked)"
            label: "D120 Overall Net Revenue (Unbaked)"
            description: "Unbaked - Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: (CASE
                          WHEN SUM(${cohort_age}) >= 120 THEN ${d120_overall_net_revenue}
                          WHEN SUM(${cohort_age}) < 120 THEN ${overall_net_revenue}
                          ELSE NULL
                        END
                  ) ;;
          }

          measure: d150_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D150 Overall Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d150_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d150_net_usd},0) ;;
          }

          measure: d150_overall_net_revenue_unbaked {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds (Unbaked)"
            label: "D150 Overall Net Revenue (Unbaked)"
            description: "Unbaked - Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: (CASE
                          WHEN SUM(${cohort_age}) >= 150 THEN ${d150_overall_net_revenue}
                          WHEN SUM(${cohort_age}) < 150 THEN ${overall_net_revenue}
                          ELSE NULL
                        END
                  ) ;;
          }

          measure: d180_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D180 Overall Net Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d180_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d180_net_usd},0) ;;
          }

          measure: d180_overall_net_revenue_unbaked {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds (Unbaked)"
            label: "D180 Overall Revenue (Unbaked)"
            description: "Unbaked - Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: (CASE
                          WHEN SUM(${cohort_age}) >= 180 THEN ${d180_overall_net_revenue}
                          WHEN SUM(${cohort_age}) < 180 THEN ${overall_net_revenue}
                          ELSE NULL
                        END
                  ) ;;
          }

          measure: d210_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D210 Overall Net Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d210_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d210_net_usd},0) ;;
          }

          measure: d210_overall_net_revenue_unbaked {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds (Unbaked)"
            label: "D210 Overall Net Revenue (Unbaked)"
            description: "Unbaked - Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: (CASE
                          WHEN SUM(${cohort_age}) >= 210 THEN ${d210_overall_net_revenue}
                          WHEN SUM(${cohort_age}) < 210 THEN ${overall_net_revenue}
                          ELSE NULL
                        END
                  ) ;;
          }

          measure: d240_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D240 Overall Net Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d240_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d240_net_usd},0) ;;
          }

          measure: d240_overall_net_revenue_unbaked {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds (Unbaked)"
            label: "D240 Overall Net Revenue (Unbaked)"
            description: "Unbaked - Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: (CASE
                        WHEN SUM(${cohort_age}) >= 240 THEN ${d240_overall_net_revenue}
                        WHEN SUM(${cohort_age}) < 240 THEN ${overall_net_revenue}
                        ELSE NULL
                        END
                  ) ;;
          }

          measure: d270_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D270 Overall Net Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d270_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d270_net_usd},0) ;;
          }

          measure: d270_overall_net_revenue_unbaked {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds (Unbaked)"
            label: "D270 Overall Net Revenue (Unbaked)"
            description: "Unbaked - Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: (CASE
                          WHEN SUM(${cohort_age}) >= 270 THEN ${d270_overall_net_revenue}
                          WHEN SUM(${cohort_age}) < 270 THEN ${overall_net_revenue}
                          ELSE NULL
                        END
                  ) ;;
          }

          measure: d300_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D300 Overall Net Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d300_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d300_net_usd},0) ;;
          }

          measure: d300_overall_net_revenue_unbaked {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds (Unbaked)"
            label: "D300 Overall Net Revenue (Unbaked)"
            description: "Unbaked - Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: (CASE
                          WHEN SUM(${cohort_age}) >= 300 THEN ${d300_overall_net_revenue}
                          WHEN SUM(${cohort_age}) < 300 THEN ${overall_net_revenue}
                          ELSE NULL
                        END
                  ) ;;
          }

          measure: d330_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D330 Overall Net Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d330_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d330_net_usd},0) ;;
          }

          measure: d330_overall_net_revenue_unbaked {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds (Unbaked)"
            label: "D330 Overall Net Revenue (Unbaked)"
            description: "Unbaked - Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: (CASE
              WHEN SUM(${cohort_age}) >= 330 THEN ${d330_overall_net_revenue}
              WHEN SUM(${cohort_age}) < 330 THEN ${overall_net_revenue}
              ELSE NULL
            END
      ) ;;
          }

          measure: d360_overall_net_revenue {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds"
            label: "D360 Overall Net Revenue"
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: COALESCE(${d360_ad_revenue},0) + COALESCE(${campaign_cohort_device_activity.d360_net_usd},0) ;;
          }

          measure: d360_overall_net_revenue_unbaked {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue Proceeds (Unbaked)"
            label: "D360 Overall Net Revenue (Unbaked)"
            description: "Unbaked - Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql: (CASE
              WHEN SUM(${cohort_age}) >= 360 THEN ${d360_overall_net_revenue}
              WHEN SUM(${cohort_age}) < 360 THEN ${overall_net_revenue}
              ELSE NULL
            END
      ) ;;
          }

          # Cumulative D[x] Overall ARPU

          measure: d0_overall_arpu {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ARPU"
            label: "D00 ARPU"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE(${d0_overall_net_revenue}/NULLIF(${campaign_cohort_device_activity.d0_cohort_size},0)::FLOAT,0) ;;
          }

          measure: d1_overall_arpu {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ARPU"
            label: "D01 ARPU"
            description: "D[x] Overall Revenue (Net IAP Revenue and Ad Monetization Revenue) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE(${d1_overall_net_revenue}/NULLIF(${campaign_cohort_device_activity.d1_cohort_size},0)::FLOAT,0) ;;
          }

          measure: d3_overall_arpu {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ARPU"
            label: "D03 ARPU"
            description: "D[x] Overall Revenue (Net IAP Revenue and Ad Monetization Revenue) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE(${d3_overall_net_revenue}/NULLIF(${campaign_cohort_device_activity.d3_cohort_size},0)::FLOAT,0) ;;
          }

          measure: d7_overall_arpu {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ARPU"
            label: "D07 ARPU"
            description: "D[x] Overall Revenue (Net IAP Revenue and Ad Monetization Revenue) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE(${d7_overall_net_revenue}/NULLIF(${campaign_cohort_device_activity.d7_cohort_size},0)::FLOAT,0) ;;
          }

          measure: d14_overall_arpu {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ARPU"
            label: "D14 ARPU"
            description: "D[x] Overall Revenue (Net IAP Revenue and Ad Monetization Revenue) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE(${d14_overall_net_revenue}/NULLIF(${campaign_cohort_device_activity.d14_cohort_size},0)::FLOAT,0) ;;
          }

          measure: d21_overall_arpu {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ARPU"
            label: "D21 ARPU"
            description: "D[x] Overall Revenue (Net IAP Revenue and Ad Monetization Revenue) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE(${d21_overall_net_revenue}/NULLIF(${campaign_cohort_device_activity.d21_cohort_size},0)::FLOAT,0) ;;
          }

          measure: d30_overall_arpu {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ARPU"
            label: "D30 ARPU"
            description: "D[x] Overall Revenue (Net IAP Revenue and Ad Monetization Revenue) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE(${d30_overall_net_revenue}/NULLIF(${campaign_cohort_device_activity.d30_cohort_size},0)::FLOAT,0) ;;
          }

          measure: d60_overall_arpu {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ARPU"
            label: "D60 ARPU"
            description: "D[x] Overall Revenue (Net IAP Revenue and Ad Monetization Revenue) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE(${d60_overall_net_revenue}/NULLIF(${campaign_cohort_device_activity.d60_cohort_size},0)::FLOAT,0) ;;
          }

          measure: d90_overall_arpu {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ARPU"
            label: "D90 ARPU"
            description: "D[x] Overall Revenue (Net IAP Revenue and Ad Monetization Revenue) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE(${d90_overall_net_revenue}/NULLIF(${campaign_cohort_device_activity.d90_cohort_size},0)::FLOAT,0) ;;
          }

          measure: d180_overall_arpu {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ARPU"
            label: "D180 ARPU"
            description: "D[x] Overall Revenue (Net IAP Revenue and Ad Monetization Revenue) / D[x] Installs (src = MMP)"
            value_format_name: usd
            sql: COALESCE(${d180_overall_net_revenue}/NULLIF(${campaign_cohort_device_activity.d180_cohort_size},0)::FLOAT,0) ;;
          }

          # OVERALL ROAS - Return on Ad Spend

          # I put this here to keep the singular dependent metrics together.
          measure: overall_roas {
            type: number
            view_label: "Singular"
            group_label: "Overall Net Revenue"
            label: "ROAS"
            description: "Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${overall_net_revenue} / NULLIF(${campaign_cohort_cost.adn_cost},0)::FLOAT),0) ;;
          }

          measure: d0_overall_roas {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ROAS"
            label: "D00 ROAS"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d0_overall_net_revenue} / NULLIF(${campaign_cohort_cost.adn_cost},0)::FLOAT),0) ;;
          }

          measure: d1_overall_roas {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ROAS"
            label: "D01 ROAS"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d1_overall_net_revenue} / NULLIF(${campaign_cohort_cost.d1_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d3_overall_roas {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ROAS"
            label: "D03 ROAS"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d3_overall_net_revenue} / NULLIF(${campaign_cohort_cost.d3_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d7_overall_roas {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ROAS"
            label: "D07 ROAS"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d7_overall_net_revenue} / NULLIF(${campaign_cohort_cost.d7_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d14_overall_roas {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ROAS"
            label: "D14 ROAS"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d14_overall_net_revenue} / NULLIF(${campaign_cohort_cost.d14_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d15_overall_roas {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ROAS"
            label: "D15 ROAS"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d15_overall_net_revenue} / NULLIF(${campaign_cohort_cost.d15_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d30_overall_roas {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ROAS"
            label: "D30 ROAS"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d30_overall_net_revenue} / NULLIF(${campaign_cohort_cost.d30_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d60_overall_roas {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ROAS"
            label: "D60 ROAS"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d60_overall_net_revenue} / NULLIF(${campaign_cohort_cost.d60_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d90_overall_roas {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ROAS"
            label: "D90 ROAS"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d90_overall_net_revenue} / NULLIF(${campaign_cohort_cost.d90_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d120_overall_roas {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ROAS"
            label: "D120 ROAS"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d120_overall_net_revenue} / NULLIF(${campaign_cohort_cost.d120_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d150_overall_roas {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ROAS"
            label: "D150 ROAS"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d150_overall_net_revenue} / NULLIF(${campaign_cohort_cost.d150_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d180_overall_roas {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ROAS"
            label: "D180 ROAS"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d180_overall_net_revenue} / NULLIF(${campaign_cohort_cost.d180_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d210_overall_roas {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ROAS"
            label: "D210 ROAS"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d210_overall_net_revenue} / NULLIF(${campaign_cohort_cost.d210_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d240_overall_roas {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ROAS"
            label: "D240 ROAS"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue)) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d240_overall_net_revenue} / NULLIF(${campaign_cohort_cost.d240_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d270_overall_roas {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ROAS"
            label: "D270 ROAS"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d270_overall_net_revenue} / NULLIF(${campaign_cohort_cost.d270_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d300_overall_roas {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ROAS"
            label: "D300 ROAS"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d300_overall_net_revenue} / NULLIF(${campaign_cohort_cost.d300_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d330_overall_roas {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ROAS"
            label: "D330 ROAS"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d330_overall_net_revenue} / NULLIF(${campaign_cohort_cost.d330_adn_cost},0)::FLOAT),0) ;;
          }

          measure: d360_overall_roas {
            type: number
            view_label: "Cumulative D[x] - Overall"
            group_label: "Net IAP & Ad Revenue ROAS"
            label: "D360 ROAS"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d360_overall_net_revenue} / NULLIF(${campaign_cohort_cost.d360_adn_cost},0)::FLOAT),0) ;;
          }

          ############################################Revenue - without NULLS
          measure: d1_overall_revenue_no_nulls {
            label: "D1 Overall Net Revenue - No Nulls"
            group_label: "Overall Net Revenue Values"
            view_label: "Weekly Report Calculations"
            type: number
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql:  COALESCE ((${d1_overall_net_revenue}),0) ;;
          }

          measure: d3_overall_revenue_no_nulls {
            label: "D3 Overall Net Revenue - No Nulls"
            group_label: "Overall Net Revenue Values"
            view_label: "Weekly Report Calculations"
            type: number
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql:  COALESCE ((${d3_overall_net_revenue}),0) ;;
          }

          measure: d7_overall_revenue_no_nulls {
            label: "D7 Overall Net Revenue - No Nulls"
            group_label: "Overall Net Revenue Values"
            view_label: "Weekly Report Calculations"
            type: number
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql:   COALESCE (SUM(${d7_overall_net_revenue}),0) ;;
          }

          measure: d14_overall_revenue_no_nulls {
            label: "D14 Overall Net Revenue - No Nulls"
            group_label: "Overall Net Revenue Values"
            view_label: "Weekly Report Calculations"
            type: number
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql:  COALESCE (SUM(${d14_overall_net_revenue}),0) ;;
          }

          measure: d21_overall_revenue_no_nulls {
            label: "D21 Overall Net Revenue - No Nulls"
            group_label: "Overall Net Revenue Values"
            view_label: "Weekly Report Calculations"
            type: number
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql:  COALESCE (SUM(${d21_overall_net_revenue}),0) ;;
          }

          measure: d30_overall_revenue_no_nulls {
            label: "D30 Overall Net Revenue - No Nulls"
            group_label: "Overall Net Revenue Values"
            view_label: "Weekly Report Calculations"
            type: number
            description: "Cumulative sum from life to D[x] (Net IAP Revenue and Ad Monetization Revenue)"
            value_format_name: usd_0
            sql:  COALESCE (SUM(${d30_overall_net_revenue}),0) ;;
          }

          measure: d1_overall_revenue_roas_no_nulls {
            type: number
            view_label: "Weekly Report Calculations"
            group_label: "Overall Net Revenue ROAS Calculations"
            label: "D1 Overall Net Revenue ROAS - no nulls - %"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d1_overall_revenue_no_nulls} / NULLIF(${campaign_cohort_cost.adn_cost}::int,0)::FLOAT),0) ;;
          }

          measure: d3_overall_revenue_roas_no_nulls {
            type: number
            view_label: "Weekly Report Calculations"
            group_label: "Overall Net Revenue ROAS Calculations"
            label: "D3 Overall Net Revenue ROAS - no nulls - %"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d3_overall_revenue_no_nulls} / NULLIF(${campaign_cohort_cost.adn_cost}::int,0)::FLOAT),0) ;;
          }

          measure: d7_overall_revenue_roas_No_nulls {
            type: number
            view_label: "Weekly Report Calculations"
            group_label: "Overall Net Revenue ROAS Calculations"
            label: "D7 Overall Net Revenue ROAS - no nulls - %"
            description: "D[x] Overall Net Revenue (Net IAP Revenue and Ad Monetization Revenue) / Cost (src = MMP)"
            value_format_name: percent_2
            sql: COALESCE((${d7_overall_revenue_no_nulls} / NULLIF(${campaign_cohort_cost.adn_cost}::int,0)::FLOAT),0) ;;
          }

          #############################################################################################################
          # OVERALL MEASURES - end
          #############################################################################################################

        }
