package co.wethinkcode.logisticsconnect;

import org.apache.commons.text.WordUtils;
import org.jetbrains.annotations.NotNull;

import java.time.LocalDate;
import java.util.*;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

public class Hub {

    private String hub_id;
    private String Province;
    private String sorting_center;
    private boolean active;
    private static final Set<String> Ids = new HashSet<>();

    @JsonCreator
    public Hub(
            @JsonProperty("hub_id") String hub_id,
            @JsonProperty("province") String Province,
            @JsonProperty("sorting_center") String sorting_center,
            @JsonProperty("active") boolean active) {
        this.hub_id = hub_id;
        this.Province = Province;
        this.sorting_center = sorting_center;
        this.active = active;
    }

    public String getHub_id() {
        return hub_id;
    }


    public String getProvince() {
        return Province;
    }

    public String getSorting_center() {
        return sorting_center;
    }

    public boolean isActive() {
        return active;
    }

    public static List<Hub> parseAndCleanRow(List<String> Lines){
        List<Hub> hublines= new ArrayList<>();


        for (int row = 1 ; row < Lines.size();row++){

            List<String> line = Arrays.asList(Lines.get(row).split(","));
            if (isDuplicateId(line.get(0))){
                continue;
            }
            hublines.add(new Hub(
                    line.get(0).trim(),
                    resolveProvince(line.get(1)),
                    capitalize(line.get(2)).trim(),
                    checkActivation(line.get(3))
            ));

        }
        return hublines;


    }

    private static final Map<String, String> PROVINCE_LOOKUP = new HashMap<>();
    static {
        PROVINCE_LOOKUP.put("gauteng", "Gauteng");
        PROVINCE_LOOKUP.put("westerncape", "Western Cape");
        PROVINCE_LOOKUP.put("easterncape", "Eastern Cape");
        PROVINCE_LOOKUP.put("kwazulunatal", "KwaZulu-Natal");
        PROVINCE_LOOKUP.put("freestate", "Free State");
        PROVINCE_LOOKUP.put("limpopo", "Limpopo");
        PROVINCE_LOOKUP.put("mpumalanga", "Mpumalanga");
        PROVINCE_LOOKUP.put("northwest", "North West");
        PROVINCE_LOOKUP.put("northerncape", "Northern Cape");
    }

    protected static String normalizeProvince(String raw) {
        if (raw == null) return null;
        return raw.toLowerCase().replaceAll("[^a-z]", "");
    }

    protected static String resolveProvince(String raw) {
        String key = normalizeProvince(raw);
        return PROVINCE_LOOKUP.get(key); // returns null if no match found
    }

    protected static String capitalize(String province){
        String TitleProvince = WordUtils.capitalize(province);
        return TitleProvince;
    }



    protected static boolean isDuplicateId(String id) {
        return !Ids.add(id);
    }

    protected static boolean checkActivation(String active){

        if (active == null) {
            return false;
        }
        String[] True = {"y", "yes", "1", "true"};
        String[] False = {"n", "no", "0", "false"};

        String normalizedActive = active.trim().toLowerCase();

// Convert arrays to Lists temporarily to use the .contains() method
        if (Arrays.asList(True).contains(normalizedActive)) {
            return true;
        } else if (Arrays.asList(False).contains(normalizedActive)) {
            return false;
        }

        return false;
    }

}
