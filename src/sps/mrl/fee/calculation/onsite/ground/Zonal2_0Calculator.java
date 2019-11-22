package sps.mrl.fee.calculation.onsite.ground;

/**
 * Zonal2_0Calculator
 *
 * @author Chilseasai@
 */
public class Zonal2_0Calculator {

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
        return 2.48;
    }

    private double calculateLargeStandardSize(final double shippingWeight) {
        if (shippingWeight <= 1.0) {
            return 3.33;
        } else if (shippingWeight > 1.0 && shippingWeight <= 2.0) {
            return 3.69;
        } else { // shippingWeight > 2.0
            return 3.69 + 0.11 * (shippingWeight - 2.0);
        }
    }

    private double calculateSmallOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 3.69;
        } else {
            return 3.69 + 0.11 * (shippingWeight - 2.0);
        }
    }

    private double calculateMediumOversize(final double shippingWeight) {
        if (shippingWeight <= 2.0) {
            return 9.11;
        } else {
            return 9.11 + 0.24 * (shippingWeight - 2.0);
        }
    }

    private double calculateLargeOversize(final double shippingWeight) {
        if (shippingWeight <= 90.0) {
            return 31.12;
        } else {
            return 31.12 + 0.35 * (shippingWeight - 90.0);
        }
    }
}
