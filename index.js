/**
 * Function: tapqueue_ArchiveOrderFunction
 * Author  : Alias M Nidhin Peter
 */
console.log("Archive Order Function");

/** 
 * Setup required libraries
 * 1. AWS SDK
 * 2. DynamoDB Document Client
 * 3. S3
 * 4. lambda
 */
let AWS = require('aws-sdk');
let client = new AWS.DynamoDB.DocumentClient({
    region: process.env.AWS_REGION
});
let S3 = new AWS.S3({
    region: process.env.AWS_REGION
});
let lambda = new AWS.Lambda({
    region: process.env.AWS_REGION
});
let json2csv = require('json2csv');
let moment = require('moment');
let each = require('async-each-series');


/**
 * Main Function for Lambda
 */
exports.handler = function (event, context, callback) {
    let list = [];

    /**
     * Check the event is an array and contains data
     */
    if (event.constructor === Object && Object.keys(event).length > 1) {

        let TTL = calculateTime();

        let ordersArray = [];

        let rsItemArray = [];
        let giftItemArray = [];
        let amenitiesItemArray = [];

        let rsOrderArray = [];
        let giftOrderArray = [];
        let guestOrderArray = [];
        let amenitiesOrderArray = [];
        let internetOrderArray = [];
        let alertOrderArray = [];
        let activityArray = [];

        let rsrecommentedArray = [];
        let gsrecommentedArray = [];
        let rsoptionsArray = [];
        let gsoptionsArray = [];

        let optionTotal = 0;
        let recTotal = 0;
        let totalValue = 0;

        let order = {};


        /**
         * Loop through event records and process uuid
         */
        each(event.Event, function (uuid, next) {
            console.log("uuid======================>", uuid);

            //for get oreder full deatils  in ORDER_STATUS_TABLE
            let statusTableParams = {
                TableName: process.env["ORDER_STATUS_TABLE"],
                Key: {
                    OrderUUID: uuid
                },
            };

            /**
             * Handle the get method && call the result
             */
            let resultStatus = client.get(statusTableParams).promise();
            resultStatus.then((statusData) => {

                let activityParameters = {
                    TableName: process.env.ORDER_ACTIVITY_TABLE,
                    ExpressionAttributeNames: {
                        "#OrderUUID": "OrderUUID"
                    },
                    ExpressionAttributeValues: {
                        ":OrderUUID": uuid
                    },
                    KeyConditionExpression: "#OrderUUID = :OrderUUID"
                };

                /**
                 * read Activity Loags 
                 */
                let resultActivity = client.query(activityParameters).promise();
                resultActivity.then((activityData) => {

                    if (statusData.Item) {
                        order = statusData.Item;

                        //for redshift
                        if (order.CustomerData.OrderType == "RoomService") {
                            order.CustomerData.OrderType = "Room Service";
                        } else if (order.CustomerData.OrderType == "Internet") {
                            order.CustomerData.OrderType = "Internet Plan";
                        }

                        if (order.CustomerData.OrderCategory == "RoomService") {
                            order.CustomerData.OrderCategory = "Room Service";
                        } else if (order.CustomerData.OrderCategory == "Internet") {
                            order.CustomerData.OrderCategory = "Internet Plan";
                        }

                        //call promice fonctions
                        writeRecordOrders()
                            .then(itemsLoop)
                            .then(writeRecord)
                            .then(writeActivityLog)
                            .then(writeS3ToBackUpData)
                            .then(function () {
                                optionTotal = 0;
                                recTotal = 0;
                                totalValue = 0;

                                activityData.Items.forEach(function (activity) {
                                    /**
                                     * Prepare item to update the ORDER STATUS TABLE
                                     */
                                    let updateParameters = {
                                        TableName: process.env["ORDER_ACTIVITY_TABLE"],
                                        Key: {
                                            OrderUUID: activity.OrderUUID,
                                            ActivityTime: activity.ActivityTime
                                        },
                                        AttributeUpdates: {
                                            "TTL": {
                                                "Action": "PUT",
                                                "Value": TTL
                                            }
                                        }
                                    };

                                    /**
                                     * Handle the put method via a Promise && call the updateStatusResult
                                     */
                                    let updateResult = client.update(updateParameters).promise();
                                    list.push(updateResult);
                                });


                                /**
                                 * Prepare item to update the ORDER STATUS TABLE
                                 */

                                let updateViewParameters = {
                                    TableName: process.env["ORDER_VIEW_TABLE"],
                                    Key: {
                                        CustomerId: String(order.CustomerId),
                                        OrderUUID: uuid
                                    },
                                    AttributeUpdates: {
                                        "TTL": {
                                            "Action": "PUT",
                                            "Value": TTL
                                        }
                                    }
                                };

                                /**
                                 * Handle the put method via a Promise && call the updateStatusResult
                                 */
                                let updateViewResult = client.update(updateViewParameters).promise();
                                list.push(updateViewResult);


                                /**
                                 * Prepare item to update the ORDER STATUS TABLE
                                 */

                                let updateStatusParameters = {
                                    TableName: process.env["ORDER_STATUS_TABLE"],
                                    Key: {
                                        OrderUUID: uuid
                                    },
                                    AttributeUpdates: {
                                        "TTL": {
                                            "Action": "PUT",
                                            "Value": TTL
                                        }
                                    }
                                };

                                /**
                                 * Handle the put method via a Promise && call the updateStatusResult
                                 */
                                let updateStatusResult = client.update(updateStatusParameters).promise();
                                list.push(updateStatusResult);
                                next();
                            })
                            .catch(function (error) {
                                //for WriteLogData
                                event.WriteLogData.ResultReason = error;
                                console.log(error);
                                // Callback on error  
                                callback(error, event);
                                invokeWriteLogFunction(error, event.WriteLogData);
                            });
                    }

                    /**
                     * For Orders table
                     */
                    function writeRecordOrders() {

                        return new Promise(function (resolve, reject) {
                            let elapsedTime = moment().diff(moment(order.CreatedTime), 'minutes') * 60;
                            let processingTime = moment(event.CompleteTime).diff(moment(order.CreatedTime), 'minutes') * 60;

                            let orders = {
                                order_key: order.OrderUUID,
                                order_type: ((order.CustomerData.OrderType == "GuestService") ? order.OrderItems[0].fprdt_fcat_id : order.CustomerData.OrderCategory),
                                order_name: ((order.CustomerData.OrderType == "GuestService") ? order.CustomerData.OrderCategory : ""),
                                location_id: order.CustomerData.LocationID,
                                location_name: order.CustomerData.LocationName,
                                language_id: order.OrderData.lang_id,
                                language: order.OrderData.language,
                                site_id: order.CustomerId,
                                elapsed_time: elapsedTime,
                                time: moment(order.CreatedTime).format('YYYY-MM-DD HH:mm:ss'),
                                processing_time: processingTime,
                                completed_time: moment(event.CompleteTime).format('YYYY-MM-DD HH:mm:ss'),
                                completed_by: event.WriteLogData.UserName,
                                required: ((order.RequiredTime != "1970-01-01T00:00:01.000Z") ? order.RequiredTime : "Now")
                            };
                            ordersArray.push(orders);

                            resolve();
                        });
                    }



                    /**
                     * For items table
                     */
                    function itemsLoop() {

                        return new Promise(function (resolve, reject) {
                            let itemObj = {};

                            Object.keys(order.OrderItems).forEach(function (key) {
                                console.log("order.OrderItems[key]=========>>>>>>>>>", order.OrderItems[key])

                                let optionID = [];
                                let recommentedID = [];

                                if (order.CustomerData.OrderType == "Room Service") {
                                    totalValue += parseInt(order.OrderItems[key].order_price);
                                    optionTotal += parseInt(order.OrderItems[key].option_total);
                                    recTotal += parseInt(order.OrderItems[key].recommented_total);

                                    // Room Service set options 
                                    if (order.OrderItems[key].options.length) {
                                        Object.keys(order.OrderItems[key].options).forEach(function (optionKey) {

                                            let options = {
                                                ordered_option_id: order.OrderItems[key].options[optionKey].fprdt_options,
                                                ordered_option_name: order.OrderItems[key].options[optionKey].option_name ? order.OrderItems[key].options[optionKey].option_name.replace("\t", "") : "",
                                                option_value: order.OrderItems[key].options[optionKey].option_price,
                                                ordered_optiontype_name: order.OrderItems[key].options[optionKey].option_type_name ? order.OrderItems[key].options[optionKey].option_type_name.replace("\t", "") : "",
                                                rds_order_id: order.OrderUUID,
                                                order_key: order.OrderUUID,
                                                ordered_item: order.OrderItems[key].fprdt_id
                                            };

                                            rsoptionsArray.push(options);
                                            optionID.push(order.OrderItems[key].options[optionKey].fprdt_options);
                                        });
                                    }
                                    // Room Service set recommented 
                                    if (order.OrderItems[key].recommented.length) {
                                        Object.keys(order.OrderItems[key].recommented).forEach(function (recommentedKey) {

                                            let recOptionsArray = [];
                                            // recommented options 
                                            if (order.OrderItems[key].recommented[recommentedKey].options) {
                                                Object.keys(order.OrderItems[key].recommented[recommentedKey].options).forEach(function (recOptions) {

                                                    let options = {
                                                        ordered_option_id: order.OrderItems[key].recommented[recommentedKey].options[recOptions].fprdt_options,
                                                        ordered_option_name: order.OrderItems[key].recommented[recommentedKey].options[recOptions].option_name ? order.OrderItems[key].recommented[recommentedKey].options[recOptions].option_name.replace("\t", "") : "",
                                                        option_value: order.OrderItems[key].recommented[recommentedKey].options[recOptions].option_price,
                                                        ordered_optiontype_name: order.OrderItems[key].recommented[recommentedKey].options[recOptions].option_type_name ? order.OrderItems[key].recommented[recommentedKey].options[recOptions].option_type_name.replace("\t", "") : "",
                                                        rds_order_id: order.OrderUUID,
                                                        order_key: order.OrderUUID,
                                                        ordered_item: order.OrderItems[key].fprdt_id
                                                    };
                                                    rsoptionsArray.push(options);
                                                    recOptionsArray.push(order.OrderItems[key].recommented[recommentedKey].options[recOptions].fprdt_options);
                                                });
                                            }
                                            let recommended_item = {
                                                ordered_recomd_id: order.OrderItems[key].recommented[recommentedKey].fprdt_rec_id,
                                                ordered_recomd_name: order.OrderItems[key].recommented[recommentedKey].rec_item_name ? order.OrderItems[key].recommented[recommentedKey].rec_item_name.replace("\t", "") : "",
                                                recomd_value: order.OrderItems[key].recommented[recommentedKey].rec_item_price,
                                                recommended_options: (recOptionsArray.toString()).replace(/[[\]]/g, ''),
                                                rds_order_id: order.OrderUUID,
                                                order_key: order.OrderUUID,
                                                ordered_item: order.OrderItems[key].fprdt_id,
                                                rec_variant_name: order.OrderItems[key].recommented[recommentedKey].rec_pricevary_item_name
                                            };
                                            rsrecommentedArray.push(recommended_item);
                                            recommentedID.push(order.OrderItems[key].recommented[recommentedKey].fprdt_rec_id);
                                        });
                                    }

                                    //rms - item
                                    itemObj = {
                                        location_id: order.CustomerData.LocationID,
                                        location_name: order.CustomerData.LocationName,
                                        language_id: order.OrderData.lang_id,
                                        language: order.OrderData.language,
                                        site_id: order.CustomerId,
                                        orderitem: order.OrderItems[key].fprdt_id,
                                        orderitem_name: order.OrderItems[key].ItemName ? order.OrderItems[key].ItemName.replace("\t", "") : "",
                                        orderitem_price: order.OrderItems[key].item_price,
                                        variant_id: order.OrderItems[key].fprdt_varyopt_id ? order.OrderItems[key].fprdt_varyopt_id : "",
                                        variant_name: order.OrderItems[key].price_variation_optionname ? order.OrderItems[key].price_variation_optionname.replace("\t", "") : '',
                                        total_value: order.OrderItems[key].order_price,
                                        option_value: order.OrderItems[key].option_total,
                                        rec_value: order.OrderItems[key].recommented_total,
                                        variant_value: order.OrderItems[key].price_variation_optionprice ? order.OrderItems[key].price_variation_optionprice : "",
                                        rms_order_key: order.OrderUUID,
                                        recomend_item: (recommentedID.toString()).replace(/[[\]]/g, ''),
                                        options: (optionID.toString()).replace(/[[\]]/g, ''),
                                        time: moment(order.CreatedTime).format('YYYY-MM-DD HH:mm:ss'),
                                        orderitem_currentlang_name: order.OrderItems[key].ItemName ? order.OrderItems[key].ItemName.replace("\t", "") : "",
                                        orderitem_desciption: order.OrderItems[key].item_description ? order.OrderItems[key].item_description : '',
                                        menu_id: (order.OrderItems[key].menuid == null || order.OrderItems[key].menuid == 'null') ? 0 : order.OrderItems[key].menuid,
                                        menu_name: order.OrderItems[key].menu_name ? order.OrderItems[key].menu_name.replace("\t", "") : "",
                                        rds_order_id: order.OrderUUID,
                                        options_status: order.OrderItems[key].options.length ? 1 : 0,
                                        recommended_items: "test" //toDo
                                    };
                                    rsItemArray.push(itemObj);

                                } else if (order.CustomerData.OrderType == "GIFT SHOP") {
                                    totalValue += parseInt(order.OrderItems[key].order_price);
                                    optionTotal += parseInt(order.OrderItems[key].option_total);
                                    recTotal += parseInt(order.OrderItems[key].recommented_total);

                                    // GIFT SHOP set options 
                                    if (order.OrderItems[key].options.length) {
                                        Object.keys(order.OrderItems[key].options).forEach(function (optionKey) {

                                            let options = {
                                                ordered_option_id: order.OrderItems[key].options[optionKey].fprdt_options,
                                                ordered_option_name: order.OrderItems[key].options[optionKey].option_name ? order.OrderItems[key].options[optionKey].option_name.replace("\t", "") : "",
                                                option_value: order.OrderItems[key].options[optionKey].option_price,
                                                ordered_optiontype_name: order.OrderItems[key].options[optionKey].option_type_name ? order.OrderItems[key].options[optionKey].option_type_name.replace("\t", "") : "",
                                                rds_order_id: order.OrderUUID,
                                                order_key: order.OrderUUID,
                                                ordered_item: order.OrderItems[key].fprdt_id
                                            };

                                            gsoptionsArray.push(options);
                                            optionID.push(order.OrderItems[key].options[optionKey].fprdt_options);
                                        });
                                    }
                                    // GIFT SHOP set recommented 

                                    if (order.OrderItems[key].recommented.length) {
                                        Object.keys(order.OrderItems[key].recommented).forEach(function (recommentedKey) {
                                            let recommended_item = {
                                                ordered_recomd_id: order.OrderItems[key].recommented[recommentedKey].fprdt_rec_id,
                                                ordered_recomd_name: order.OrderItems[key].recommented[recommentedKey].rec_item_name ? order.OrderItems[key].recommented[recommentedKey].rec_item_name.replace("\t", "") : "",
                                                recomd_value: order.OrderItems[key].recommented[recommentedKey].rec_item_price,
                                                rds_order_id: order.OrderUUID,
                                                order_key: order.OrderUUID,
                                                ordered_item: order.OrderItems[key].fprdt_id,
                                                rec_variant_name: order.OrderItems[key].recommented[recommentedKey].rec_pricevary_item_name
                                            };
                                            gsrecommentedArray.push(recommended_item);
                                            recommentedID.push(order.OrderItems[key].recommented[recommentedKey].fprdt_rec_id);
                                        });
                                    }

                                    //rms - item
                                    itemObj = {
                                        location_id: order.CustomerData.LocationID,
                                        location_name: order.CustomerData.LocationName,
                                        language_id: order.OrderData.lang_id,
                                        language: order.OrderData.language,
                                        site_id: order.CustomerId,
                                        orderitem: order.OrderItems[key].fprdt_id,
                                        orderitem_name: order.OrderItems[key].ItemName ? order.OrderItems[key].ItemName.replace("\t", "") : "",
                                        orderitem_price: order.OrderItems[key].item_price,
                                        variant_id: order.OrderItems[key].fprdt_varyopt_id ? order.OrderItems[key].fprdt_varyopt_id : "",
                                        variant_name: order.OrderItems[key].price_variation_optionname ? order.OrderItems[key].price_variation_optionname.replace("\t", "") : '',
                                        total_value: order.OrderItems[key].order_price,
                                        option_value: order.OrderItems[key].option_total,
                                        rec_value: order.OrderItems[key].recommented_total,
                                        variant_value: order.OrderItems[key].price_variation_optionprice ? order.OrderItems[key].price_variation_optionprice : "",
                                        rms_order_key: order.OrderUUID,
                                        recomend_item: (recommentedID.toString()).replace(/[[\]]/g, ''),
                                        options: (optionID.toString()).replace(/[[\]]/g, ''),
                                        time: moment(order.CreatedTime).format('YYYY-MM-DD HH:mm:ss'),
                                        orderitem_currentlang_name: order.OrderItems[key].ItemName ? order.OrderItems[key].ItemName.replace("\t", "") : "",
                                        orderitem_desciption: order.OrderItems[key].item_description ? order.OrderItems[key].item_description : '',
                                        menu_id: (order.OrderItems[key].menuid == null || order.OrderItems[key].menuid == 'null') ? 0 : order.OrderItems[key].menuid,
                                        menu_name: order.OrderItems[key].menu_name ? order.OrderItems[key].menu_name.replace("\t", "") : "",
                                        options_status: order.OrderItems[key].options.length ? 1 : 0,
                                        recommended_items: "test", //toDo
                                        rds_order_id: order.OrderUUID
                                    };
                                    giftItemArray.push(itemObj);

                                } else if (order.CustomerData.OrderType == "AMENITIES") {
                                    totalValue += parseInt(order.OrderItems[key].item_price);

                                    itemObj = {
                                        category_id: order.OrderItems[key].fprdt_fcat_id,
                                        category_name: order.OrderItems[key].gs_category_name,
                                        location_id: order.CustomerData.LocationID,
                                        location_name: order.CustomerData.LocationName,
                                        language_id: order.OrderData.lang_id,
                                        language: order.OrderData.language,
                                        amenity_rds_id: order.OrderItems[key].fprdt_id,
                                        amenity_name: order.OrderItems[key].item_Name_en ? order.OrderItems[key].item_Name_en.replace("\t", "") : "",
                                        price: order.OrderItems[key].item_price,
                                        current_amenitylang_name: order.OrderItems[key].ItemName ? order.OrderItems[key].ItemName.replace("\t", "") : "",
                                        site_id: order.CustomerId,
                                        time: moment(order.CreatedTime).format('YYYY-MM-DD HH:mm:ss'),
                                        amenities_order_key: order.OrderUUID,
                                        rds_order_id: order.OrderUUID //toDo
                                    };
                                    amenitiesItemArray.push(itemObj);
                                }
                            });
                            resolve();
                        });
                    }

                    /**
                     * For item_order table
                     */
                    function writeRecord() {


                        return new Promise(function (resolve, reject) {
                            let itemObj = {};

                            if (order.CustomerData.OrderType == "Room Service") {
                                itemObj = {
                                    rms_order_key: order.OrderUUID,
                                    location_id: order.CustomerData.LocationID,
                                    location_name: order.CustomerData.LocationName,
                                    language_id: order.OrderData.lang_id,
                                    language: order.OrderData.language,
                                    site_id: order.CustomerId,
                                    guest_count: order.OrderData.guest_no,
                                    order_type: order.CustomerData.OrderCategory,
                                    total_value: totalValue ? totalValue : 0,
                                    option_value: optionTotal ? optionTotal : 0,
                                    rec_value: recTotal ? recTotal : 0,
                                    time: moment(order.CreatedTime).format('YYYY-MM-DD HH:mm:ss'),
                                    service_charge: order.OrderData.service_charge ? parseInt(order.OrderData.service_charge) : 0,
                                    special_request: order.OrderData.requests
                                };
                                rsOrderArray.push(itemObj);

                            } else if (order.CustomerData.OrderType == "GIFT SHOP") {
                                itemObj = {
                                    gs_order_key: order.OrderUUID,
                                    location_id: order.CustomerData.LocationID,
                                    location_name: order.CustomerData.LocationName,
                                    language_id: order.OrderData.lang_id,
                                    language: order.OrderData.language,
                                    site_id: order.CustomerId,
                                    order_type: order.CustomerData.OrderCategory,
                                    total_value: totalValue ? totalValue : 0,
                                    option_value: optionTotal ? optionTotal : 0,
                                    rec_value: recTotal ? recTotal : 0,
                                    time: moment(order.CreatedTime).format('YYYY-MM-DD HH:mm:ss'),
                                    special_request: order.OrderData.requests,
                                };
                                giftOrderArray.push(itemObj);
                            } else if (order.CustomerData.OrderType == "AMENITIES") {
                                itemObj = {
                                    amenities_rds_id: "", //toDo
                                    amenities_order_key: order.OrderUUID,
                                    location_id: order.CustomerData.LocationID,
                                    location: order.CustomerData.LocationName,
                                    language_id: order.OrderData.lang_id,
                                    language: order.OrderData.language,
                                    site_id: order.CustomerId,
                                    total_value: totalValue ? totalValue : 0,
                                    time: moment(order.CreatedTime).format('YYYY-MM-DD HH:mm:ss')
                                };
                                amenitiesOrderArray.push(itemObj);
                            } else if (order.CustomerData.OrderType == "GuestService") {
                                itemObj = {
                                    location_id: order.CustomerData.LocationID,
                                    location_name: order.CustomerData.LocationName,
                                    language_id: order.OrderData.lang_id,
                                    language: order.OrderData.language,
                                    site_id: order.CustomerId,
                                    time: moment(order.CreatedTime).format('YYYY-MM-DD HH:mm:ss'),
                                    gus_order_key: order.OrderUUID,
                                    order_category: order.OrderItems[0].fprdt_fcat_id,
                                    order_category_name: order.OrderItems[0].gs_category_name,
                                    order_guestitem: order.OrderItems[0].fprdt_id,
                                    order_guestitem_name: order.OrderItems[0].item_Name_en ? order.OrderItems[0].item_Name_en.replace("\t", "") : "",
                                    order_guestitem_currentlang_name: order.OrderItems[0].ItemName ? order.OrderItems[0].ItemName.replace("\t", "") : "",
                                    order_guestitem_description: order.OrderItems[0].gs_textentry_label ? order.OrderItems[0].gs_textentry_label : "",
                                    order_guestitem_title: order.OrderItems[0].gs_textentry_title ? order.OrderItems[0].gs_textentry_title : "",
                                    rds_order_id: order.OrderUUID //toDo
                                };
                                guestOrderArray.push(itemObj);
                            } else if (order.CustomerData.OrderType == "Internet Plan") {
                                itemObj = {
                                    location_id: order.CustomerData.LocationID,
                                    location_name: order.CustomerData.LocationName,
                                    language_id: order.OrderData.lang_id,
                                    language: order.OrderData.language,
                                    site_id: order.CustomerId,
                                    time: moment(order.CreatedTime).format('YYYY-MM-DD HH:mm:ss'),
                                    order_key: order.OrderUUID,
                                    rds_order_id: order.OrderUUID, //toDo                                    
                                    price: order.OrderItems[0].item_price,
                                    plan_name: order.OrderItems[0].item_Name_en ? order.OrderItems[0].item_Name_en.replace("\t", "") : "",
                                    used_data: "",
                                    message: "",
                                    plan_id: order.OrderItems[0].fprdt_id,
                                    plan_currentlang_name: order.OrderItems[0].ItemName
                                };
                                internetOrderArray.push(itemObj);
                            } else if (order.CustomerData.OrderType == "alert") {
                                itemObj = {
                                    location_id: order.CustomerData.LocationID,
                                    location_name: order.CustomerData.LocationName,
                                    language_id: order.OrderData.lang_id,
                                    language: order.OrderData.language,
                                    site_id: order.CustomerId,
                                    time: moment(order.CreatedTime).format('YYYY-MM-DD HH:mm:ss'),
                                    order_key: order.OrderUUID,
                                    rds_order_id: order.OrderUUID, //toDo                                                     
                                    device_item_name: order.OrderItems[0].item_Name_en ? order.OrderItems[0].item_Name_en.replace("\t", "") : "",
                                    device_currentlang_name: order.OrderItems[0].ItemName ? order.OrderItems[0].ItemName.replace("\t", "") : ""
                                };
                                alertOrderArray.push(itemObj);
                            }
                            resolve();
                        });
                    }


                    /**
                     * Write Activity Log on s3 
                     */
                    function writeActivityLog() {


                        return new Promise(function (resolve, reject) {
                            activityData.Items.forEach(function (activity) {
                                let log = {
                                    order_key: activity.OrderUUID,
                                    log_status: activity.ActivityData.NewStatus,
                                    log_user: activity.UserName,
                                    site_id: activity.CustomerId,
                                    time: moment(activity.ActivityTime).format('YYYY-MM-DD HH:mm:ss'),
                                };
                                activityArray.push(log);
                            });
                            resolve();
                        });
                    }


                    /**
                     * For write BackUp to s3
                     */
                    function writeS3ToBackUpData() {

                        return new Promise(function (resolve, reject) {
                            let date = new Date();
                            let Key = date.getFullYear() + "/" + date.getMonth() + 1 + "/" + date.getDate() + "/" + uuid + "/" + uuid + ".csv";
                            let activityFields = ['ActivityData', 'ActivityTime', 'CustomerId', 'OrderUUID', 'UserName'];
                            let statusFields = ['CreatedTime', 'CustomerData', 'CustomerId', 'GroupId', 'LocationName', 'Options', 'OrderCategory', 'OrderData', 'OrderItems', 'OrderStatus', 'OrderType', 'OrderUUID', 'RequiredTime'];

                            json2csv({
                                data: statusData.Item,
                                fields: statusFields
                            }, function (err, statusCsv) {
                                if (err) {
                                    console.log(err, err); // an error occurred
                                    reject(err);
                                } else {

                                    let uploadStatusConfig = {
                                        Bucket: process.env.ARCHIVE_S3_BUCKET_NAME,
                                        // ACL: "public-read",
                                        ContentType: "application/vnd.ms-excel",
                                        Body: statusCsv,
                                        Key: Key
                                    };
                                    // Upload to S3
                                    S3.upload(uploadStatusConfig).send((err, data) => {
                                        if (err) {
                                            console.log("Error Occured in uploading file----------->>>>>>>>> :", err);
                                            event.WriteLogData.ResultReason = err;
                                            invokeWriteLogFunction(err, event.WriteLogData);
                                        } else {
                                            console.log("Successfully files uploaded--------------->>>>>>>>> :", data);

                                            json2csv({
                                                data: activityData.Items,
                                                fields: activityFields
                                            }, function (err, activityCsv) {
                                                if (err) {
                                                    console.log(err, err); // an error occurred
                                                    reject(err);
                                                } else {
                                                    console.log(activityCsv);

                                                    let Key = date.getFullYear() + "/" + date.getMonth() + "/" + date.getDate() + "/" + uuid + "/" + uuid + "_activityLog.csv";
                                                    let uploadActivityConfig = {
                                                        Bucket: process.env.ARCHIVE_S3_BUCKET_NAME,
                                                        // ACL: "public-read",
                                                        ContentType: "application/vnd.ms-excel",
                                                        Body: activityCsv,
                                                        Key: Key
                                                    };
                                                    // Upload to S3
                                                    S3.upload(uploadActivityConfig).send((err, data) => {
                                                        if (err) {
                                                            console.log("Error Occured in uploading file----------->>>>>>>>> :", err);
                                                            event.WriteLogData.ResultReason = err;
                                                            invokeWriteLogFunction(err, event.WriteLogData);
                                                        } else {
                                                            console.log("Successfully files uploaded--------------->>>>>>>>> :", data);
                                                            resolve();
                                                        }
                                                    });
                                                }
                                            });
                                        }
                                    });
                                }
                            });
                        });
                    } // End write BackUp to s3

                });

            });
        }, function (err) {

            if (!err) {

                //For Order.csv
                if (ordersArray.length) {
                    let fields = ['order_key', 'order_type', 'order_name', 'location_id', 'location_name', 'language_id', 'language', 'site_id', 'elapsed_time', 'time', 'processing_time', 'completed_time', 'completed_by', 'required'];
                    let tableName = "orders.csv";
                    csvUploadInS3(ordersArray, fields, tableName);
                }

                //For item .csv
                if (rsItemArray.length) {
                    let fields = ['location_id', 'location_name', 'language_id', 'language', 'site_id', 'orderitem', 'orderitem_name', 'orderitem_price', 'variant_id', 'variant_name', 'total_value', 'option_value', 'rec_value', 'variant_value', 'rms_order_key', 'recomend_item', 'options', 'time', 'orderitem_currentlang_name', 'orderitem_desciption', 'menu_id', 'menu_name', 'rds_order_id', 'options_status', 'recommended_items'];
                    let tableName = "roomservice_item.csv";
                    csvUploadInS3(rsItemArray, fields, tableName);
                }
                if (giftItemArray.length) {
                    let fields = ['location_id', 'location_name', 'language_id', 'language', 'site_id', 'orderitem', 'orderitem_name', 'orderitem_price', 'variant_id', 'variant_name', 'total_value', 'option_value', 'rec_value', 'variant_value', 'rms_order_key', 'recomend_item', 'options', 'time', 'orderitem_currentlang_name', 'orderitem_desciption', 'menu_id', 'menu_name', 'options_status', 'recommended_items', 'rds_order_id'];
                    let tableName = "gift_shop_product.csv";
                    csvUploadInS3(giftItemArray, fields, tableName);
                }
                if (amenitiesItemArray.length) {
                    let fields = ['category_id', 'category_name', 'location_id', 'location_name', 'language_id', 'language', 'amenity_rds_id', 'amenity_name', 'price', 'current_amenitylang_name', 'site_id', 'time', 'amenities_order_key', 'rds_order_id'];
                    let tableName = "amenities.csv";
                    csvUploadInS3(amenitiesItemArray, fields, tableName);
                }

                //For iteam Order.csv
                if (rsOrderArray.length) {
                    let fields = ['rms_order_key', 'location_id', 'location_name', 'language_id', 'language', 'site_id', 'guest_count', 'order_type', 'total_value', 'option_value', 'rec_value', 'time', 'service_charge', 'special_request'];
                    let tableName = "roomservice_order.csv";
                    csvUploadInS3(rsOrderArray, fields, tableName);
                }
                if (giftOrderArray.length) {
                    let fields = ['gs_order_key', 'location_id', 'location_name', 'language_id', 'language', 'site_id', 'order_type', 'total_value', 'option_value', 'rec_value', 'time', 'special_request'];
                    let tableName = "giftshop_order.csv";
                    csvUploadInS3(giftOrderArray, fields, tableName);
                }
                if (guestOrderArray.length) {
                    let fields = ['location_id', 'location_name', 'language_id', 'language', 'site_id', 'time', 'gus_order_key', 'order_category', 'order_category_name', 'order_guestitem', 'order_guestitem_name', 'order_guestitem_currentlang_name', 'order_guestitem_description', 'order_guestitem_title', 'rds_order_id'];
                    let tableName = "guestservice_order.csv";
                    csvUploadInS3(guestOrderArray, fields, tableName);
                }
                if (amenitiesOrderArray.length) {
                    let fields = ['amenities_rds_id', 'amenities_order_key', 'location_id', 'location', 'language_id', 'language', 'site_id', 'total_value', 'time'];
                    let tableName = "amenities_order.csv";
                    csvUploadInS3(amenitiesOrderArray, fields, tableName);
                }
                if (internetOrderArray.length) {
                    let fields = ['location_id', 'location_name', 'language_id', 'language', 'site_id', 'time', 'order_key', 'rds_order_id', 'price', 'plan_name', 'used_data', 'message', 'plan_id', 'plan_currentlang_name'];
                    let tableName = "internet_order.csv";
                    csvUploadInS3(internetOrderArray, fields, tableName);
                }
                if (alertOrderArray.length) {
                    let fields = ['location_id', 'location_name', 'language_id', 'language', 'site_id', 'time', 'order_key', 'rds_order_id', 'device_item_name', 'device_currentlang_name'];
                    let tableName = "devicemanage_order.csv";
                    csvUploadInS3(alertOrderArray, fields, tableName);
                }

                // For activity.csv
                if (activityArray.length) {
                    let fields = ['order_key', 'log_status', 'log_user', 'site_id', 'time'];
                    let tableName = "order_archive_log.csv";
                    csvUploadInS3(activityArray, fields, tableName);
                }





                /**
                 * Handle the array of promises
                 */
                Promise.all(list).then(() => {
                    console.log("Successfully updated TTL");
                    setTimeout(function () {
                        list = [];
                        // For Rs optionsArray csv
                        if (rsoptionsArray.length) {
                            let fields = ['ordered_option_id', 'ordered_option_name', 'option_value', 'ordered_optiontype_name', 'rds_order_id', 'order_key', 'ordered_item'];
                            let tableName = "options.csv";
                            csvUploadInS3(rsoptionsArray, fields, tableName);
                        }
                        if (gsoptionsArray.length) {
                            let fields = ['ordered_option_id', 'ordered_option_name', 'option_value', 'ordered_optiontype_name', 'rds_order_id', 'order_key', 'ordered_item'];
                            let tableName = "gsp_options.csv";
                            csvUploadInS3(gsoptionsArray, fields, tableName);
                        }

                        // For Rs recommentedArray csv
                        if (rsrecommentedArray.length) {
                            let fields = ['ordered_recomd_id', 'ordered_recomd_name', 'recomd_value', 'recommended_options', 'rds_order_id', 'order_key', 'ordered_item', 'rec_variant_name'];
                            let tableName = "recommended_item.csv";
                            csvUploadInS3(rsrecommentedArray, fields, tableName);
                        }
                        if (gsrecommentedArray.length) {
                            let fields = ['ordered_recomd_id', 'ordered_recomd_name', 'recomd_value', 'rds_order_id', 'order_key', 'ordered_item', 'rec_variant_name'];
                            let tableName = "recommended_gsp.csv";
                            csvUploadInS3(gsrecommentedArray, fields, tableName);
                        }

                        /**
                         * Handle the array of promises
                         */
                        Promise.all(list).then(() => {
                            console.log("Completed");
                            callback(null, event);
                            // invokeWriteLogFunction(null, event.WriteLogData);
                        });

                    }, 5000);

                });
            } else {
                console.log("err-->>>", err);
            }

        });


    }

    /**
     * invoke WriteLog Function 
     */
    function invokeWriteLogFunction(err, WriteLog) {

        let WriteLogData = WriteLog;

        WriteLogData.ProcessName = 'tapqueue_ArchiveOrderFunction';
        WriteLogData.Result = 'Success';

        if (err) {
            WriteLogData.Result = "Error";
            WriteLogData.ResultReason = err;
        }

        lambda.invoke({
            FunctionName: process.env.WRITE_LOG_FUNCTION,
            Payload: JSON.stringify({
                WriteLogData: WriteLogData
            }, null, 2) // pass params
        }, function (error, data) {
            if (error) {
                context.done('error', error);
            }
            if (data.Payload) {
                console.log("invokeWriteLogFunction");
                return;
            }
        });
    }





    /**
     * upload csv in s3
     * 
     * @param {*} uploadConfig 
     */
    function csvUploadInS3(iteamArray, fields, fileName) {

        let updateResult = new Promise(function (resolve, reject) {
            json2csv({
                data: iteamArray,
                fields: fields,
                del: '~',
                quotes: '',
                hasCSVColumnTitle: true
            }, function (err, csv) {
                if (err) {
                    console.log(err, err); // an error occurred
                    reject(err);
                } else {
                    console.log(csv); // successful response

                    let uploadConfig = {
                        Bucket: process.env.S3_BUCKET_NAME, //toDo
                        // ACL: "public-read",
                        ContentType: "text/csv",
                        Body: csv,
                        Key: new Date().getTime() + "-" + fileName
                    };
                    S3.upload(uploadConfig).send((err, data) => {
                        if (err) {
                            console.log("Error Occured in uploading file :", err);
                            reject(err);
                        } else {
                            console.log("Upload successfully---------");
                            resolve();
                        }
                    });
                }
            });
        });

        list.push(updateResult);
    }


};


/**
 * Calculate the time that the 
 * 1. By taking the required time AND
 * 2. Rounding down to the closest 45 minute interval
 */
function calculateTime() {

    let dateTime = new Date();
    let seconds = 0,
        milliseconds = 0;
    let minutes = dateTime.getMinutes();
    let day = dateTime.getDate() + 2;

    minutes = minutes - (minutes % 5);
    dateTime.setMilliseconds(milliseconds);
    dateTime.setSeconds(seconds);
    dateTime.setMinutes(minutes);
    dateTime.setDate(day);

    let TTL = dateTime / 1000;
    return TTL;
}