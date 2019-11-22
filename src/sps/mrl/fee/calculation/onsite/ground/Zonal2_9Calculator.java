package sps.mrl.fee.calculation.onsite.ground;

/**
 * Zonal2_9Calculator
 *
 * @author Chilseasai@
 */
public class Zonal2_9Calculator {

    public double getAmount(final TestCaseItem item) {
        switch (item.getSizeTier()) {
            case "UsSmallStandardSize":
                return calculateSmallStandardSize(item.getShippingWeight());
            case "UsLargeStandardSize":
                return calculateLargeStandardSize(item.getShippingWeight());
            case "UsSmallOversize":
                return calculateSmallOversize(item.getShippingWeight());
            case "UsMediumOversize":
                return calculateMediumOversize(item.getShippingWeight());
            case "UsLargeOversize":
                return calculateLargeOversize(item.getShippingWeight());
            default:
                return 0.0;
        }
    }

    private double calculateSmallStandardSize(final double shippingWeight) {
        return 2.61;
    }

    private double calculateLargeStandardSize(final double shippingWeight) {
        if (shippingWeight <= 1.0) {
            return 3.76;
        } else if (shippingWeight > 1.0 && shippingWeight <= 2.0) {
            return 4.21;
        } else { // shippingWeight > 2.0
            return 4.21 + 0.17 * (shippingWeight - 2.0);
        }
    }

    private double calculateSmallOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 4.21;
        } else {
            return 4.21 + 0.17 * (shippingWeight - 2.0);
        }
    }

    private double calculateMediumOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 9.63;
        } else {
            return 9.63 + 0.28 * (shippingWeight - 2.0);
        }
    }

    private double calculateLargeOversize(final double shippingWeight) {
        if (shippingWeight <= 90.0) {
            return 31.64;
        } else {
            return 31.64 + 0.37 * (shippingWeight - 90.0);
        }
    }
}
