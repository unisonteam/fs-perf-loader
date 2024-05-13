package team.unison.perf;

import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import team.unison.perf.fswrapper.LocalFs;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;


public class SaveAndLoadMD5HashTest {
  private static final String BUCKET_NAME = "test";
  private static final String TMP_FILE_NAME = "tmp_file";
  private MessageDigest md5;
  private LocalFs localFs;

  // https://www.kermitproject.org/utf8.html
  private static final String[] TEST_DATA = {
      "€",
      "I can eat glass and it doesn't hurt me.",                 // English
      "Я могу есть стекло, оно мне не вредит.",                  // Russian
      "⠊⠀⠉⠁⠝⠀⠑⠁⠞⠀⠛⠇⠁⠎⠎⠀⠁⠝⠙⠀⠊⠞⠀⠙⠕⠑⠎⠝⠞⠀⠓⠥⠗⠞⠀⠍⠑",          // English (Braille)
      "Mogę jeść szkło, i mi nie szkodzi.",                      // Polish
      "Pot să mănânc sticlă și ea nu mă rănește.",               // Romanian
      "Μπορώ να φάω σπασμένα γυαλιά χωρίς να πάθω τίποτα.",      // Greek
      "ᠪᠢ ᠰᠢᠯᠢ ᠢᠳᠡᠶᠦ ᠴᠢᠳᠠᠨᠠ ᠂ ᠨᠠᠳᠤᠷ ᠬᠣᠤᠷᠠᠳᠠᠢ ᠪᠢᠰᠢ",              // Mongolian (Classic)
      "我能吞下玻璃而不傷身體。",                                     // Chinese (Traditional)
      "私はガラスを食べられます。それは私を傷つけません。",                // Japanese
      "Կրնամ ապակի ուտել և ինծի անհանգիստ չըներ։",               // Armenian
      "나는 유리를 먹을 수 있어요. 그래도 아프지 않아요",                    // Korean
      "काचं शक्नोम्यत्तुम् । नोपहिनस्ति माम् ॥",                                 // Sanskrit
      "᚛᚛ᚉᚑᚅᚔᚉᚉᚔᚋ ᚔᚈᚔ ᚍᚂᚐᚅᚑ ᚅᚔᚋᚌᚓᚅᚐ᚜",             // Old Irish (Ogham)
      "Ég get etið gler án þess að meiða mig.",                  // Íslenska / Icelandic
      "ᛁᚳ᛫ᛗᚨᚷ᛫ᚷᛚᚨᛋ᛫ᛖᚩᛏᚪᚾ᛫ᚩᚾᛞ᛫ᚻᛁᛏ᛫ᚾᛖ᛫ᚻᛖᚪᚱᛗᛁᚪᚧ᛫ᛗᛖ᛬",               // Anglo-Saxon (Runes)
      "मैं काँच खा सकता हूँ और मुझे उससे कोई चोट नहीं पहुंचती. ",                // Hindi (masculine)
      "நான் கண்ணாடி சாப்பிடுவேன், அதனால் எனக்கு ஒரு கேடும் வராது",   // Tamil
      "මට වීදුරු කෑමට හැකියි. එයින් මට කිසි හානියක් සිදු නොවේ. ",          // Sinhalese
      "أنا قادر على أكل الزجاج و هذا لا يؤلمني."                    // Arabic
  };

  private static Stream<Arguments> provideTestStrings() {
    return Arrays.stream(TEST_DATA).map(Arguments::of);
  }

  @BeforeEach
  public void setup() throws NoSuchAlgorithmException {
    localFs = new LocalFs();
    md5 = MessageDigest.getInstance("MD5");
  }

  @AfterEach
  public void cleanup() {
    localFs.deleteAllFilesRecursively();
  }

  @Test
  public void testEmptyString() {
    String data = "";
    byte[] hash = md5.digest(data.getBytes(StandardCharsets.UTF_8));
    localFs.writeFile(BUCKET_NAME, TMP_FILE_NAME, hash);
    byte[] bytes = localFs.readFile(BUCKET_NAME, TMP_FILE_NAME);
    assertArrayEquals(hash, bytes);
  }

  @ParameterizedTest
  @MethodSource("provideTestStrings")
  public void testNonEmptyString(String data) {
    byte[] expectedHash = md5.digest(data.getBytes(StandardCharsets.UTF_8));
    localFs.writeFile(BUCKET_NAME, TMP_FILE_NAME, expectedHash);
    byte[] actualHash = localFs.readFile(BUCKET_NAME, TMP_FILE_NAME);
    assertArrayEquals(expectedHash, actualHash);
  }
}
