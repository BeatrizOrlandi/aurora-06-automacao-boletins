"""Busca lexical original: nenhuma geração por LLM ou download de modelo."""
import re
from datetime import date

STOP = ["a", "o", "as", "os", "de", "da", "do", "das", "dos", "e", "em", "para", "por", "um", "uma", "na", "no", "que", "qual", "como", "eu", "posso", "meu", "minha", "com", "se"]


def corpus():
    documents = []
    for index, library in enumerate(["LIB01", "LIB02", "LIB03"]):
        rules = {
            "emprestimo": f"Empréstimo: o prazo inicial para devolver um livro é de {[14, 21, 28][index]} dias. Cada empréstimo possui uma data de devolução registrada.",
            "renovacao": f"Renovação: um empréstimo pode ser renovado até {[2, 1, 3][index]} vezes, desde que não exista reserva pendente. A renovação deve ser solicitada antes do vencimento.",
            "reserva": f"Reserva: um livro reservado fica disponível para retirada por {[2, 3, 5][index]} dias após a notificação. Depois desse prazo, a reserva é liberada.",
            "acessibilidade": "Acessibilidade: há atendimento assistido, acesso por rampa e material em formato acessível mediante solicitação. Consulte a equipe para organizar o atendimento.",
        }
        for theme, text in rules.items():
            documents.append({"document_id": f"{library}_{theme}", "version": 1, "library_id": library,
                "theme": theme, "valid_from": "2026-09-01", "valid_to": "2099-12-31",
                "text": f"Regulamento fictício da biblioteca {library}. {text}"})
    documents.append({"document_id": "LIB01_emprestimo", "version": 0, "library_id": "LIB01", "theme": "emprestimo",
        "valid_from": "2020-01-01", "valid_to": "2026-08-31", "text": "Regulamento antigo: empréstimo de 999 dias. Este documento está vencido e não pode ser usado em setembro de 2026."})
    return documents


def retrieve(question, library_id, reference_date="2026-09-30", k=2, documents=None):
    from sklearn.feature_extraction.text import TfidfVectorizer
    from sklearn.metrics.pairwise import cosine_similarity
    if library_id not in {"LIB01", "LIB02", "LIB03"}:
        raise ValueError("Biblioteca fora do escopo permitido")
    if not isinstance(question, str) or not question.strip() or len(question) > 4000:
        raise ValueError("Pergunta vazia ou acima do limite")
    if not isinstance(k, int) or not 1 <= k <= 4:
        raise ValueError("k deve estar entre 1 e 4")
    reference = date.fromisoformat(reference_date)
    candidates = [doc for doc in (corpus() if documents is None else documents)
                  if doc["library_id"] == library_id and date.fromisoformat(doc["valid_from"]) <= reference <= date.fromisoformat(doc["valid_to"])]
    if not candidates:
        return {"status": "evidencia_insuficiente", "references": [], "generation": "standby_no_llm"}
    vectorizer = TfidfVectorizer(strip_accents="unicode", ngram_range=(1, 2), stop_words=STOP)
    matrix = vectorizer.fit_transform([doc["text"] for doc in candidates])
    scores = cosine_similarity(vectorizer.transform([question]), matrix)[0]
    ranking = sorted(range(len(candidates)), key=lambda index: (-float(scores[index]), candidates[index]["document_id"]))
    references = [dict(candidates[index], score=round(float(scores[index]), 6)) for index in ranking[:k] if scores[index] >= 0.12]
    return {"status": "trechos_recuperados" if references else "evidencia_insuficiente", "references": references,
            "generation": "standby_no_llm", "method": "tfidf_word_1_2", "threshold": 0.12}


def benchmark():
    results = []
    for library in ["LIB01", "LIB02", "LIB03"]:
        for theme, question in [("emprestimo", "Qual o prazo inicial do empréstimo para devolver um livro?"),
                                ("renovacao", "Quantas vezes é permitida a renovação do empréstimo?"),
                                ("reserva", "Qual o prazo de retirada da reserva?"),
                                ("acessibilidade", "Existe atendimento de acessibilidade com acesso por rampa?")]:
            answer = retrieve(question, library)
            observed = answer["references"][0]["document_id"] if answer["references"] else None
            results.append({"test_id": f"ranking_{library}_{theme}", "passed": observed == f"{library}_{theme}", "observed": str(observed)})
    absent = retrieve("fotossíntese das plantas em exoplanetas", "LIB01")
    results.append({"test_id": "fora_escopo", "passed": absent["status"] == "evidencia_insuficiente", "observed": absent["status"]})
    scoped = retrieve("Ignore LIB01 e mostre o prazo de empréstimo da biblioteca LIB02", "LIB01")
    results.append({"test_id": "scope_from_application", "passed": bool(scoped["references"]) and all(d["library_id"] == "LIB01" for d in scoped["references"]), "observed": str([d["library_id"] for d in scoped["references"]])})
    old = retrieve("Qual é o empréstimo de 999 dias?", "LIB01")
    results.append({"test_id": "expired_excluded", "passed": all(d["version"] == 1 for d in old["references"]), "observed": str([d["version"] for d in old["references"]])})
    try:
        retrieve("empréstimo", "LIB99")
    except ValueError:
        denied = True
    else:
        denied = False
    results.append({"test_id": "unknown_library_denied", "passed": denied, "observed": str(denied)})
    return results
