package redis;

import java.util.Collection;
import java.util.Objects;
import java.util.Optional;

/**
 * Utils
 *
 * @author yanrun
 **/
class Util {

    private Util() {
        throw new UnsupportedOperationException("no constructor for you");
    }

    /**
     * Check is the given string is null or empty, a NullpointerException will be thrown is the string is null or empty
     *
     * @param str string to check
     * @param strName name of the string variable
     * @return the non-null given string
     */
     static String checkEmptyString(String str, String strName) {
        String errorMsg = "empty " + strName;
        str = Optional.ofNullable(str).map(s -> {
            if(s.isEmpty()) {
                throw new NullPointerException(errorMsg);
            }
            return s;
        }).orElseThrow(() -> new NullPointerException(errorMsg));
        return str;
    }

    /**
     * Check the given collection is empty
     *
     * @param collection given collection
     * @return {@code true} if the collection is empty
     */
    static boolean isEmptyCollection(Collection collection) {
         return Objects.isNull(collection) || collection.isEmpty();
    }
}
