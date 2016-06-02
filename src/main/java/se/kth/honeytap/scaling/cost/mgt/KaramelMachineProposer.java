package se.kth.honeytap.scaling.cost.mgt;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import se.kth.honeytap.scaling.Constants;
import se.kth.honeytap.scaling.exceptions.DBConnectionFailureException;
import se.kth.honeytap.scaling.group.Group;
import se.kth.honeytap.scaling.models.MachineType;
import se.kth.honeytap.scaling.utils.DBUtil;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * Created with IntelliJ IDEA.
 *
 * @author Ashansa Perera
 * @version $Id$
 * @since 1.0
 */
public class KaramelMachineProposer implements MachineProposer {

    private Connection dbConnection = null;
    private static final Log log = LogFactory.getLog(KaramelMachineProposer.class);
    private static final String AWS_INSTANCES_TABLE = "awsec2_instance";
    private static final String NAME_COLUMN = "NAME";  //varchar
    private static final String ECU_COLUMN = "ECU";   //varchar
    private static final String MEMORYGIB_COLUMN = "MEMORYGIB";  //float
    private static final String STORAGEGB_COLUMN = "STORAGEGB";  //varchar
    private static final String VCPU_COLUMN = "VCPU";   //int

    private static final String AWSEC2_INSTANCES_PRICE_TABLE = "awsec2_instance_price";
    private static final String AWSEC2INSTANCE_NAME_COLUMN = "AWSEC2INSTANCE_NAME";
    private static final String REGION_COLUMN = "REGION";
    private static final String PURCHASEOPTION_COLUMN = "PURCHASEOPTION";
    private static final String PRICE_COLUMN = "PRICE";
    private static final String ONDEMAND_HOURLY = "ODHourly";

    private static final String AWSEC2_SPOT_INSTANCE_TABLE = "awsec2_spot_instance";
    private static final String INSTANCE_TYPE_COLUMN = "INSTANCETYPE";
    private static final String AVAILABILITYZONE_COLUMN = "AVAILABILITYZONE";

    private static final String ONDEMAND = "ONDEMAND";
    private static final String SPOT = "SPOT";
    private static final String LAUNCHER = "EC2";


    public KaramelMachineProposer() {
        try {
            dbConnection = DBUtil.getKandyDbConnection();
        } catch (DBConnectionFailureException e) {
            log.error("DB Connection failure with Kandy DB. Proposing default suggestions. ", e);
        }
    }

    public MachineType[] getMachineProposals(String groupId, Map<Group.ResourceRequirement, Integer> minimumResourceReq,
                                                    int noOfMachines, float reliabilityPercentage) {

        //TODO call Kandy and get proposal

        //connecting to Kandy db and get info since kandy doesn't provide service yet
        try {
            Map<String, Float> requirementMap = new HashMap<String, Float>();
            for (Map.Entry<Group.ResourceRequirement, Integer> entry : minimumResourceReq.entrySet()) {
                if (Group.ResourceRequirement.NUMBER_OF_VCPUS.name().equals(entry.getKey().name())) {
                    requirementMap.put(VCPU_COLUMN, Float.valueOf(entry.getValue()));
                } else if (Group.ResourceRequirement.RAM.name().equals(entry.getKey().name())) {
                    requirementMap.put(MEMORYGIB_COLUMN, Float.valueOf(entry.getValue()));
                }
                /////did not add other properties since they cannot be handled by Kandy db : type string
            }

            ResultSet instanceTypesResults = getInstanceTypesForReq(requirementMap);
            ArrayList<String> instanceTypeArray = new ArrayList<String>();
            while (instanceTypesResults.next()) {
                instanceTypeArray.add(instanceTypesResults.getString(NAME_COLUMN));
            }

            String[] instanceTypes = instanceTypeArray.toArray(new String[instanceTypeArray.size()]) ;
            TreeMap<Double, ArrayList<String>> priceMap = new TreeMap<Double, ArrayList<String>>();
                                                                    //key:price  value:OD/SI:InstanceType:Region
            ArrayList<String> valueListOfKey;

            ResultSet onDemandResults = getCheapestOnDemandInstances(noOfMachines, instanceTypes);
            while (onDemandResults.next()) {
                double price = onDemandResults.getBigDecimal(PRICE_COLUMN).doubleValue();
                StringBuilder typeInstanceRegion = new StringBuilder(ONDEMAND).append(Constants.SEPARATOR).
                        append(onDemandResults.getString(AWSEC2INSTANCE_NAME_COLUMN)).append(Constants.SEPARATOR).
                        append(onDemandResults.getString(REGION_COLUMN));
                if (priceMap.containsKey(price)) {
                    valueListOfKey = priceMap.get(price);
                } else {
                    valueListOfKey = new ArrayList<String>();
                }
                valueListOfKey.add(typeInstanceRegion.toString());
                priceMap.put(price, valueListOfKey);
            }

            boolean canContainSpotInstances = (reliabilityPercentage < 100f);
            if (canContainSpotInstances) {
                ResultSet spotResults = getCheapestSpotInstances(noOfMachines, instanceTypes);
                while (spotResults.next()) {
                    double biddingPrice = spotResults.getBigDecimal(PRICE_COLUMN).doubleValue() * 2.0;
                    //TODO propose a good value for bidding price : ie - ondemand price of instance type
                    StringBuilder typeInstanceRegion = new StringBuilder(SPOT).append(Constants.SEPARATOR).
                            append(spotResults.getString(INSTANCE_TYPE_COLUMN)).append(Constants.SEPARATOR).
                            append(spotResults.getString(REGION_COLUMN));
                    if (priceMap.containsKey(biddingPrice)) {
                        valueListOfKey = priceMap.get(biddingPrice);
                    } else {
                        valueListOfKey = new ArrayList<String>();
                    }
                    valueListOfKey.add(typeInstanceRegion.toString());
                    priceMap.put(biddingPrice, valueListOfKey);
                }
            }

            MachineType[] machineProposals = getCheapestProposals(noOfMachines, priceMap, canContainSpotInstances);
            return machineProposals;
        } catch (SQLException e) {
            log.error("Error while calculating the cost effective solution. Going to propose the default solutions");
            return getDefaultProposals(noOfMachines, reliabilityPercentage);
        }
    }

    private MachineType[] getDefaultProposals(int noOfMachines, float reliabilityReq) {
        MachineType[] machineTypes = new MachineType[noOfMachines];
        HashMap<String,String> properties = new HashMap<String, String>();
        boolean isPreemptible = false;
        if (reliabilityReq < 100f) {
            isPreemptible = true;
            properties.put(Constants.BIDDING_PRICE, "1");  //hardcode bidding price to 1 doller
        }

        for (int i = 0; i < noOfMachines; ++i) {
            machineTypes[i] = new MachineType("EC2", isPreemptible, properties);
            //region or any other properties are not set, so that Karamel will use default region and instance type
        }
        return machineTypes;
    }

  /**
   *
   * @param minimumResourceReq key:column name, value:required value
   */
    private ResultSet getInstanceTypesForReq(Map<String, Float> minimumResourceReq) throws SQLException {
        StringBuilder selectQuery = new StringBuilder("select ").append(NAME_COLUMN).append(" FROM ");
        selectQuery.append(AWS_INSTANCES_TABLE).append(" where ");

        //TODO add storage consideration too
        //TODO ECU is in string format :(
        selectQuery.append(MEMORYGIB_COLUMN).append(" >= ? and ").append(VCPU_COLUMN).append(" >= ? and not (").
                append(STORAGEGB_COLUMN).append(" = ?)");

        try {
            PreparedStatement selectRuleStatement = dbConnection.prepareStatement(selectQuery.toString());
            selectRuleStatement.setFloat(1, minimumResourceReq.get(MEMORYGIB_COLUMN));
            selectRuleStatement.setInt(2, (int) Math.ceil(minimumResourceReq.get(VCPU_COLUMN)));
            selectRuleStatement.setString(3, "ebsonly");
            ResultSet resultSet = selectRuleStatement.executeQuery();
            return resultSet;
        } catch (SQLException e) {
            log.error("Error occoured while retrieving AWS instance types for required resources" + e.getMessage());
            throw e;
        }
    }

    //SELECT * FROM servicerecommender.awsec2_instance_price where PURCHASEOPTION = "ODHourly" and
                    // AWSEC2INSTANCE_NAME in ("m3.xlarge", "c3.large", "m3.large") order by PRICE limit 10;

    //SELECT servicerecommender.awsec2_instance_price.REGION,
            // servicerecommender.awsec2_instance_price.AWSEC2INSTANCE_NAME
            // FROM servicerecommender.awsec2_instance_price where PURCHASEOPTION = "ODHourly" and AWSEC2INSTANCE_NAME
            // in ("m3.xlarge", "c3.large", "m3.large") order by PRICE limit 10;
    private ResultSet getCheapestOnDemandInstances(int noOfInstances, String[] instanceTypeNames) throws SQLException {
        /*StringBuilder selectQuery = new StringBuilder("select ").append(AWSEC2INSTANCE_NAME_COLUMN).append(" ").
                append(REGION_COLUMN).append(" FROM ").append(AWSEC2_INSTANCES_PRICE_TABLE).append(" where ").
                append(PURCHASEOPTION_COLUMN).append(" = ?, ").append(AWSEC2INSTANCE_NAME_COLUMN).append(" in ");*/

        StringBuilder selectQuery = new StringBuilder("select * FROM ").append(AWSEC2_INSTANCES_PRICE_TABLE).
                append(" where ").append(PURCHASEOPTION_COLUMN).append(" = ? and ").append(AWSEC2INSTANCE_NAME_COLUMN).
                append(" in ");

        for (int i = 0; i < instanceTypeNames.length; ++i) {
            if (i == 0) {
                selectQuery.append("(?");
            } else if (i == (instanceTypeNames.length -1)) {
                selectQuery.append(",?)");
            } else {
                selectQuery.append(",?");
            }
        }
        selectQuery.append(" order by ").append(PRICE_COLUMN).append(" limit ?");

        try {
            PreparedStatement selectInstances = dbConnection.prepareStatement(selectQuery.toString());
            selectInstances.setString(1, ONDEMAND_HOURLY);
            for (int i = 1; i <= instanceTypeNames.length; ++i) {
                selectInstances.setString(1+i, instanceTypeNames[i - 1]);  //add 1 since 1 parm in already set
            }
            selectInstances.setInt(instanceTypeNames.length + 2, noOfInstances);
            ResultSet resultSet = selectInstances.executeQuery();
            return resultSet;
        } catch (SQLException e) {
            log.error("Error occoured while retrieving the cheapest on demand instances" + e.getMessage());
            throw e;
        }
    }

    //TODO should improve this further
    //SELECT * FROM servicerecommender.awsec2_spot_instance where INSTANCETYPE in ("m3.xlarge", "c3.large", "m3.large")
                //group by AVAILABILITYZONE order by PRICE limit 10;
    private ResultSet getCheapestSpotInstances(int noOfInstances, String[] instanceTypeNames) throws SQLException {
        //TODO return a map of instance type and bidding price:onDemandPrice OR onDemandPrice * .9
        StringBuilder selectQuery = new StringBuilder("select * FROM ").append(AWSEC2_SPOT_INSTANCE_TABLE).
                append(" where ").append(INSTANCE_TYPE_COLUMN).append(" in ");

        for (int i = 0; i < instanceTypeNames.length; ++i) {
            if (i == 0) {
                selectQuery.append("(?");
            } else if (i == (instanceTypeNames.length -1)) {
                selectQuery.append(",?)");
            } else {
                selectQuery.append(",?");
            }
        }
        selectQuery.append(" group by ").append(AVAILABILITYZONE_COLUMN).append(" order by ").
                append(PRICE_COLUMN).append(" limit ?");

        try {
            PreparedStatement selectInstances = dbConnection.prepareStatement(selectQuery.toString());
            for (int i = 1; i <= instanceTypeNames.length; ++i) {
                selectInstances.setString(i, instanceTypeNames[i -1]);
            }
            selectInstances.setInt(instanceTypeNames.length + 1, noOfInstances);
            ResultSet resultSet = selectInstances.executeQuery();
            return resultSet;
        } catch (SQLException e) {
            log.error("Error occoured while retrieving the cheapest spot instances" + e.getMessage());
            throw e;
        }
    }

  /**
   *
   * @param noOfMachines
   * @param priceMap
   * @param containsSpot : if there are both OD and SI for same price, can select the OD
   * @return
   */
    private MachineType[] getCheapestProposals(int noOfMachines, TreeMap<Double, ArrayList<String>> priceMap,
                                               boolean containsSpot) {
        MachineType[] machineProposals = new MachineType[noOfMachines];
        boolean done = false;
        int index = 0;
        for (Map.Entry<Double, ArrayList<String>> entry : priceMap.entrySet()) {
            if (done) {
                break;
            }
            //fill data
            ArrayList<String> machineProperties = entry.getValue();
            for (String value : machineProperties) {   //value:OD/SI:InstanceType:Region
                String[] instanceProperties = value.split(Constants.SEPARATOR);
                if (instanceProperties.length != 3) {
                    continue;
                } else {
                    //TODO - minor : if(containsSpot) { if both OD and spot for the same price and don't need all of them, get spot}
                    Map<String, String> properties = new HashMap<String, String>();
                    properties.put(MachineType.Properties.TYPE.name(), instanceProperties[1]);
                    properties.put(MachineType.EC2Properties.REGION.name(), instanceProperties[2]);

                    boolean isPreemptibale = false;
                    if (SPOT.equals(instanceProperties[0])) {
                        isPreemptibale = true;
                        properties.put(MachineType.Properties.BIDDING_PRICE.name(), String.valueOf(entry.getKey()));
                    }

                    MachineType machineType = new MachineType(LAUNCHER, isPreemptibale, properties);
                    machineProposals[index] = machineType;

                    index ++;
                    if (index == noOfMachines) {
                        done = true;
                        break;
                    }
                }
            }
        }
        return machineProposals;
    }
}
