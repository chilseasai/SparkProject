package sps.mrl.fee.calculation.onsite.ground;

/**
 * Zonal2_4Calculator
 *
 * @author Chilseasai@
 */
public class Zonal2_4Calculator {

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
        return 2.54;
    }

    private double calculateLargeStandardSize(final double shippingWeight) {
        if (shippingWeight <= 1.0) {
            return 3.52;
        } else if (shippingWeight > 1.0 && shippingWeight <= 2.0) {
            return 3.92;
        } else { // shippingWeight > 2.0
            return 3.92 + 0.14 * (shippingWeight - 2.0);
        }
    }

    private double calculateSmallOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 3.92;
        } else {
            return 3.92 + 0.14 * (shippingWeight - 2.0);
        }
    }

    private double calculateMediumOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 9.34;
        } else {
            return 9.34 + 0.26 * (shippingWeight - 2.0);
        }
    }

    private double calculateLargeOversize(final double shippingWeight) {
        if (shippingWeight <= 90.0) {
            return 31.35;
        } else {
            return 31.35 + 0.36 * (shippingWeight - 90.0);
        }
    }
}
